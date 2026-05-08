from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID
import logging

from .models import (
    ExecutionOutcome,
    Operation,
    OperationManagerEvent,
    OperationManagerEventType,
    OperationManagerState,
    TerminationReason,
)
from .notification_handler import NotificationHandler
from .request_handler import RequestHandler
from .schedule import Schedule, ScheduleSortStrategy
from .retry_policy import RetryPolicy


class _ManagerRuntimeState(str, Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    STOPPING = "stopping"


class OperationManager:
    """
    Runtime coordinator for a single-agent operation schedule.

    Responsibilities:
    - pull and start operations from `Schedule`
    - drive lifecycle transitions (complete/fail/stop/cancel)
    - run an async execution loop with pause/resume/stop control
    - emit runtime and operation events

    Integration callbacks are separated by responsibility:
    - `on_request_callback`: policy decisions for request events (`*_REQUESTED`)
    - `on_notification_callback`: best-effort handling for non-request events

    Callback expectations for request events (`*_REQUESTED`):
    - return a boolean decision quickly
    - for start handshake events (`OPERATION_START_REQUESTED`,
        `OPERATION_START_DISPATCH_REQUESTED`), only explicit `True` allows progress
        - for `OPERATION_CANCEL_REQUESTED`/`OPERATION_STOP_REQUESTED`/
            `OPERATION_RESUME_REQUESTED` events, only explicit `True` allows progress

    Request callbacks are time-bounded by `request_event_timeout_seconds`.
    If no decision is produced within the timeout (or callback raises),
    the request is treated as denied.
    """

    def __init__(
        self,
        agent_id: str,
        on_request_callback: Callable[[OperationManagerEvent], object] | None = None,
        on_notification_callback: (
            Callable[[OperationManagerEvent], object] | None
        ) = None,
        poll_interval_seconds: float = 0.1,
        start_request_max_retries: int = 5,
        start_request_retry_cooldown_seconds: float = 1.0,
        request_event_timeout_seconds: float = 5.0,
        payload_model: Any | None = None,
        schedule_sort_strategy: ScheduleSortStrategy = ScheduleSortStrategy.START_TIME_THEN_PRIORITY,
        logger: logging.Logger | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than 0")
        if start_request_max_retries <= 0:
            raise ValueError("start_request_max_retries must be greater than 0")
        if start_request_retry_cooldown_seconds < 0:
            raise ValueError(
                "start_request_retry_cooldown_seconds must be non-negative"
            )
        if request_event_timeout_seconds <= 0:
            raise ValueError("request_event_timeout_seconds must be greater than 0")

        self._logger = logger
        self._schedule = Schedule(
            agent_id=agent_id, sort_strategy=schedule_sort_strategy
        )
        self._payload_model = payload_model
        request_retry_policy = RetryPolicy(
            max_retries=start_request_max_retries,
            retry_cooldown_seconds=start_request_retry_cooldown_seconds,
        )
        self._runtime_state = _ManagerRuntimeState.STOPPED
        self._is_paused = False
        self._stop_requested = False
        self._poll_interval_seconds = poll_interval_seconds
        self._running_since: datetime | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._wakeup_event: asyncio.Event | None = None
        self._event_history: list[OperationManagerEvent] = []
        self._event_history_limit = 1000
        self._notification_handler = NotificationHandler(
            on_notification_callback=on_notification_callback,
            runtime_loop_getter=lambda: self._runtime_loop,
            logger=self._logger,
        )
        self._request_handler = RequestHandler(
            on_request_callback=on_request_callback,
            request_retry_policy=request_retry_policy,
            request_event_timeout_seconds=request_event_timeout_seconds,
            append_event_history=self._append_event_history,
            emit_event=self._emit_event,
            log_event=self._log_event,
            notify_wakeup=self._notify_wakeup,
            pause=self.pause,
        )

    @property
    def schedule(self) -> Schedule:
        return self._schedule

    @property
    def current_operation(self) -> Operation | None:
        return self._schedule.pulled_operation

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @property
    def is_running(self) -> bool:
        return self._runtime_state is not _ManagerRuntimeState.STOPPED

    @property
    def payload_model(self) -> Any | None:
        return self._payload_model

    def add(self, operation: Operation) -> None:
        self._execute_state_mutation(lambda: self._add_internal(operation))

    def _add_internal(self, operation: Operation) -> None:
        self._schedule.add(operation)
        self._emit_event(
            OperationManagerEventType.OPERATION_ADDED,
            operation=operation,
        )

    def get_schedule(self) -> list[Operation]:
        return self._schedule.list()

    def get_state(self) -> OperationManagerState:
        now = datetime.now(timezone.utc)
        running_since = self._running_since
        uptime_seconds: float | None = None
        if self.is_running and running_since is not None:
            uptime_seconds = (now - running_since).total_seconds()

        return OperationManagerState(
            is_running=self.is_running,
            is_paused=self._is_paused,
            queue_size=len(self._schedule),
            current_operation=self.current_operation,
            running_since=running_since,
            uptime_seconds=uptime_seconds,
        )

    def pause(self) -> None:
        self._execute_state_mutation(self._pause_internal)

    def _pause_internal(self) -> None:
        self._is_paused = True
        self._emit_event(OperationManagerEventType.OPERATION_MANAGER_PAUSED)

    def resume(self) -> None:
        self._execute_state_mutation(self._resume_internal)

    def _resume_internal(self) -> None:
        if self.current_operation is not None:
            if not self._request_handler.request_operation_with_retry_sync(
                self.current_operation,
                OperationManagerEventType.OPERATION_RESUME_REQUESTED,
            ):
                return

        self._request_handler.clear_all_request_retry_state()
        self._is_paused = False
        self._emit_event(OperationManagerEventType.OPERATION_MANAGER_RESUMED)

    def _start_next(self) -> Operation | None:
        if self._is_paused:
            return None

        if self.current_operation is not None:
            raise RuntimeError("an operation is already running")

        operation = self._schedule.next()
        if operation is None:
            return None

        self._emit_event(
            OperationManagerEventType.OPERATION_STARTED,
            operation=operation,
        )
        return operation

    async def run_once(self) -> Operation | None:
        if self._is_paused or self.current_operation is not None:
            return None

        next_operation = self._schedule.peek()
        if next_operation is None:
            return None

        if next_operation.time_window is not None:
            now = datetime.now(timezone.utc)
            if next_operation.time_window.start > now:
                return None

        if self._request_handler.has_request_cooldown(next_operation):
            return None

        if not await self._request_handler.request_operation_start(next_operation):
            return None

        if not await self._request_handler.request_operation_start_dispatch(
            next_operation
        ):
            return None

        operation = self._start_next()
        if operation is None:
            return None

        return operation

    async def run(self) -> None:
        if self._runtime_state is not _ManagerRuntimeState.STOPPED:
            raise RuntimeError("operation manager is already running")

        self._runtime_state = _ManagerRuntimeState.RUNNING
        self._running_since = datetime.now(timezone.utc)
        self._stop_requested = False
        self._runtime_loop = asyncio.get_running_loop()
        self._wakeup_event = asyncio.Event()
        self._emit_event(OperationManagerEventType.OPERATION_MANAGER_STARTED)
        try:
            while not self._stop_requested:
                try:
                    await self.run_once()
                except Exception as error:
                    if self._logger is not None:
                        self._logger.exception(
                            "operation manager loop iteration failed",
                            exc_info=error,
                        )

                if self._stop_requested:
                    break

                await self._wait_for_next_signal()
        finally:
            self._runtime_state = _ManagerRuntimeState.STOPPED
            self._running_since = None
            self._emit_event(OperationManagerEventType.OPERATION_MANAGER_STOPPED)
            self._runtime_loop = None
            self._wakeup_event = None

    def request_stop(self) -> None:
        self._execute_state_mutation(self._request_stop_internal)

    def _request_stop_internal(self) -> None:
        self._stop_requested = True
        if self._runtime_state is _ManagerRuntimeState.RUNNING:
            self._runtime_state = _ManagerRuntimeState.STOPPING
        self._notify_wakeup()

    def complete_current(self) -> Operation:
        return self._execute_state_mutation(self._complete_current_internal)

    def _complete_current_internal(self) -> Operation:
        operation = self._require_current_operation()
        self._schedule.complete(operation)
        self._emit_event(
            OperationManagerEventType.OPERATION_COMPLETED,
            operation=operation,
        )
        return operation

    def fail_current(self) -> Operation:
        return self._execute_state_mutation(self._fail_current_internal)

    def _fail_current_internal(self) -> Operation:
        operation = self._require_current_operation()
        operation.execution_outcome = ExecutionOutcome.FAILED
        operation.termination_reason = TerminationReason.NONE
        self._schedule.complete(operation)
        self._emit_event(
            OperationManagerEventType.OPERATION_FAILED,
            operation=operation,
        )
        return operation

    def stop_current(self) -> Operation:
        return self._execute_state_mutation(self._stop_current_internal)

    def _stop_current_internal(self) -> Operation:
        operation = self._require_current_operation()
        if not self._request_handler.request_operation_with_retry_sync(
            operation,
            OperationManagerEventType.OPERATION_STOP_REQUESTED,
        ):
            return operation

        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.STOPPED
        self._schedule.complete(operation)
        self._emit_event(
            OperationManagerEventType.OPERATION_STOPPED,
            operation=operation,
        )
        return operation

    def cancel(self, operation_id: UUID) -> Operation | None:
        return self._execute_state_mutation(lambda: self._cancel_internal(operation_id))

    def _cancel_internal(self, operation_id: UUID) -> Operation | None:
        operation = self.get_operation(operation_id)
        if operation is None:
            return None

        if not self._request_handler.request_operation_with_retry_sync(
            operation,
            OperationManagerEventType.OPERATION_CANCEL_REQUESTED,
        ):
            return None

        operation = self._schedule.cancel(operation_id)
        if operation is not None:
            self._request_handler.clear_request_retry_state(operation.id)
            self._emit_event(
                OperationManagerEventType.OPERATION_CANCELLED,
                operation=operation,
            )
        return operation

    def get_event_history(
        self, limit: int | None = None
    ) -> list[OperationManagerEvent]:
        if limit is None:
            return list(self._event_history)
        return list(self._event_history[-limit:])

    def _require_current_operation(self) -> Operation:
        operation = self.current_operation
        if operation is None:
            raise RuntimeError("no current operation")
        return operation

    def get_operation(self, operation_id: UUID) -> Operation | None:
        return self._schedule.get(operation_id)

    @staticmethod
    def _set_operation_finish_if_needed(operation: Operation) -> None:
        if operation.finish_time is None:
            operation.finish_time = datetime.now(timezone.utc)

    async def _wait_for_next_signal(self) -> None:
        wait_seconds = self._next_wait_seconds()
        if wait_seconds == 0:
            await asyncio.sleep(0)
            return

        if self._wakeup_event is None:
            await asyncio.sleep(self._poll_interval_seconds)
            return

        if wait_seconds is None:
            await self._wakeup_event.wait()
            self._wakeup_event.clear()
            return

        try:
            await asyncio.wait_for(
                self._wakeup_event.wait(),
                timeout=wait_seconds,
            )
        except TimeoutError:
            pass
        finally:
            self._wakeup_event.clear()

    def _next_wait_seconds(self) -> float | None:
        if self._is_paused:
            return None

        next_operation = self._schedule.peek()
        if next_operation is None:
            return None

        now = datetime.now(timezone.utc)

        time_window_wait_seconds = 0.0
        if next_operation.time_window is not None:
            time_window_wait_seconds = (
                next_operation.time_window.start - now
            ).total_seconds()
            if time_window_wait_seconds < 0:
                time_window_wait_seconds = 0.0

        cooldown_wait_seconds = self._request_handler.request_cooldown_wait_seconds(
            next_operation,
            now,
        )

        return max(time_window_wait_seconds, cooldown_wait_seconds)

    def _emit_event(
        self,
        event_type: OperationManagerEventType,
        operation: Operation | None = None,
        data: dict[str, Any] | None = None,
    ) -> OperationManagerEvent:
        event = OperationManagerEvent(
            event_type=event_type,
            agent_id=(
                operation.agent_id if operation is not None else self._schedule.agent_id
            ),
            operation_id=operation.id if operation is not None else None,
            operation_name=operation.name if operation is not None else None,
            data={} if data is None else data,
        )
        self._append_event_history(event)
        self._notification_handler.notify(event)
        self._log_event(event)

        self._notify_wakeup()
        return event

    def _append_event_history(self, event: OperationManagerEvent) -> None:
        self._event_history.append(event)
        if len(self._event_history) > self._event_history_limit:
            self._event_history = self._event_history[-self._event_history_limit :]

    def _log_event(self, event: OperationManagerEvent) -> None:
        if self._logger is None:
            return

        if event.event_type in {
            OperationManagerEventType.OPERATION_MANAGER_PAUSED,
            OperationManagerEventType.OPERATION_MANAGER_STOPPED,
        }:
            self._logger.warning(f"EVENT {event.event_type}")
        elif event.event_type in {
            OperationManagerEventType.OPERATION_MANAGER_RESUMED,
        }:
            self._logger.info(f"EVENT {event.event_type}")
        else:
            self._logger.debug(
                f"EVENT {event.event_type} for operation {event.operation_name} ({event.operation_id})"
            )

    def _notify_wakeup(self) -> None:
        if not self.is_running:
            return
        if self._runtime_loop is None or self._wakeup_event is None:
            return

        self._runtime_loop.call_soon_threadsafe(self._wakeup_event.set)

    def _execute_state_mutation(self, mutation: Callable[[], Any]) -> Any:
        runtime_loop = self._runtime_loop
        if runtime_loop is None or not self.is_running:
            return mutation()

        if not runtime_loop.is_running():
            return mutation()

        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is runtime_loop:
                return mutation()
        except RuntimeError:
            pass

        result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)
        done_event = threading.Event()

        def run_mutation() -> None:
            try:
                result_queue.put((True, mutation()))
            except Exception as error:
                result_queue.put((False, error))
            finally:
                done_event.set()

        runtime_loop.call_soon_threadsafe(run_mutation)
        done_event.wait()

        success, payload = result_queue.get_nowait()
        if success:
            return payload
        raise payload  # type: ignore[misc]
