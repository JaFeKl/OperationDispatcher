from __future__ import annotations

import asyncio
import logging
import queue
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from .dispatch_queue import DispatchQueue, SortRule
from .models import (
    DispatchEvent,
    EventType,
    ExecutionOutcome,
    ExecutionState,
    OperationHistoryEntry,
    OperationExecution,
    OperationDispatcherState,
    ScheduledOperation,
    TerminationReason,
)
from .notification_handler import NotificationHandler
from .request_handler import RequestHandler
from .retry_policy import RetryPolicy


class OperationDispatcher:
    """
    Runtime coordinator for one resource dispatch queue.

    Scheduling (`DispatchQueue`) and runtime execution (`OperationExecution`) are
    intentionally separated and synchronized by scheduled operation id.
    """

    def __init__(
        self,
        resource_id: str,
        on_request_callback: (
            Callable[[DispatchEvent], bool | dict[str, Any] | None] | None
        ) = None,
        on_notification_callback: Callable[[DispatchEvent], object] | None = None,
        poll_interval_seconds: float = 0.1,
        start_request_max_retries: int = 5,
        start_request_retry_cooldown_seconds: float = 1.0,
        request_event_timeout_seconds: float = 5.0,
        dispatch_queue_sort_rules: list[SortRule] | None = None,
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
        self._dispatch_queue = DispatchQueue(
            resource_id=resource_id,
            sort_rules=dispatch_queue_sort_rules,
        )
        self._executions_by_operation_id: dict[UUID, OperationExecution] = {}
        self._events_by_operation_id: dict[UUID, list[DispatchEvent]] = {}

        request_retry_policy = RetryPolicy(
            max_retries=start_request_max_retries,
            retry_cooldown_seconds=start_request_retry_cooldown_seconds,
        )

        self._is_paused = False
        self._stop_requested = False
        self._poll_interval_seconds = poll_interval_seconds
        self._running_since: datetime | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._wakeup_event: asyncio.Event | None = None
        self._event_history: list[DispatchEvent] = []
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
            append_operation_event=self._append_operation_event,
            get_execution_id=self._get_execution_id,
            emit_event=self._emit_event,
            log_event=self._log_event,
            notify_wakeup=self._notify_wakeup,
            pause=self.pause,
        )

    @property
    def dispatch_queue(self) -> DispatchQueue:
        return self._dispatch_queue

    @property
    def current_scheduled_operation(self) -> ScheduledOperation | None:
        return self._dispatch_queue.pulled_operation

    @property
    def current_operation(self) -> ScheduledOperation | None:
        return self.current_scheduled_operation

    @property
    def current_execution(self) -> OperationExecution | None:
        current = self.current_scheduled_operation
        if current is None:
            return None
        return self._executions_by_operation_id.get(current.id)

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @property
    def is_running(self) -> bool:
        runtime_loop = self._runtime_loop
        return runtime_loop is not None and runtime_loop.is_running()

    @property
    def is_stopping(self) -> bool:
        return self._stop_requested and self.is_running

    def add(self, scheduled_operation: ScheduledOperation) -> None:
        self._execute_state_mutation(lambda: self._add_internal(scheduled_operation))

    def _add_internal(self, scheduled_operation: ScheduledOperation) -> None:
        self._dispatch_queue.add(scheduled_operation)
        self._executions_by_operation_id[scheduled_operation.id] = OperationExecution(
            operation_id=scheduled_operation.id
        )
        self._events_by_operation_id.setdefault(scheduled_operation.id, [])
        self._emit_event(
            EventType.OPERATION_ADDED,
            scheduled_operation=scheduled_operation,
        )

    def get_schedule(self) -> list[ScheduledOperation]:
        return self._dispatch_queue.list()

    def get_execution(self, operation_id: UUID) -> OperationExecution | None:
        return self._executions_by_operation_id.get(operation_id)

    def get_state(self) -> OperationDispatcherState:
        now = datetime.now(timezone.utc)
        running_since = self._running_since
        uptime_seconds: float | None = None
        if self.is_running and running_since is not None:
            uptime_seconds = (now - running_since).total_seconds()

        return OperationDispatcherState(
            is_running=self.is_running,
            is_paused=self._is_paused,
            queue_size=len(self._dispatch_queue),
            current_operation=self.current_operation,
            running_since=running_since,
            uptime_seconds=uptime_seconds,
        )

    def pause(self) -> None:
        self._execute_state_mutation(self._pause_internal)

    def _pause_internal(self) -> None:
        self._is_paused = True
        self._set_current_execution_state(ExecutionState.PAUSED)
        self._emit_event(EventType.OPERATION_MANAGER_PAUSED)

    def resume(self) -> None:
        self._execute_state_mutation(self._resume_internal)

    def _resume_internal(self) -> None:
        current = self.current_scheduled_operation
        if current is not None:
            if not self._request_handler.request_operation_with_retry_sync(
                current,
                EventType.OPERATION_RESUME_REQUESTED,
            ):
                return

        self._request_handler.clear_all_request_retry_state()
        self._is_paused = False
        self._set_current_execution_state(ExecutionState.RUNNING)
        self._emit_event(EventType.OPERATION_MANAGER_RESUMED)

    def _start_next(self) -> ScheduledOperation | None:
        if self._is_paused:
            return None

        if self.current_scheduled_operation is not None:
            raise RuntimeError("an operation is already running")

        scheduled_operation = self._dispatch_queue.next()
        if scheduled_operation is None:
            return None

        execution = self._require_execution(scheduled_operation.id)
        execution.state = ExecutionState.RUNNING
        if execution.start_time is None:
            execution.start_time = datetime.now(timezone.utc)

        self._emit_event(
            EventType.OPERATION_STARTED,
            scheduled_operation=scheduled_operation,
        )
        return scheduled_operation

    async def run_once(self) -> ScheduledOperation | None:
        if self._is_paused or self.current_scheduled_operation is not None:
            return None

        next_operation = self._dispatch_queue.peek()
        if next_operation is None:
            return None

        if next_operation.release_date is not None:
            now = datetime.now(timezone.utc)
            if next_operation.release_date > now:
                return None

        if self._request_handler.has_request_cooldown(next_operation):
            return None

        if not await self._request_handler.request_operation_start(next_operation):
            return None

        return self._start_next()

    async def run(self) -> None:
        if self.is_running:
            raise RuntimeError("operation dispatcher is already running")

        self._running_since = datetime.now(timezone.utc)
        self._stop_requested = False
        self._runtime_loop = asyncio.get_running_loop()
        self._wakeup_event = asyncio.Event()
        self._emit_event(EventType.OPERATION_MANAGER_STARTED)
        try:
            while not self._stop_requested:
                try:
                    await self.run_once()
                except Exception as error:
                    if self._logger is not None:
                        self._logger.exception(
                            "operation dispatcher loop iteration failed",
                            exc_info=error,
                        )

                if self._stop_requested:
                    break

                await self._wait_for_next_signal()
        finally:
            self._running_since = None
            self._emit_event(EventType.OPERATION_MANAGER_STOPPED)
            self._runtime_loop = None
            self._wakeup_event = None

    def request_stop(self) -> None:
        self._execute_state_mutation(self._request_stop_internal)

    def _request_stop_internal(self) -> None:
        self._stop_requested = True
        self._notify_wakeup()

    def complete_current(self) -> ScheduledOperation:
        return self._execute_state_mutation(self._complete_current_internal)

    def _complete_current_internal(self) -> ScheduledOperation:
        scheduled_operation = self._require_current_scheduled_operation()
        execution = self._require_execution(scheduled_operation.id)
        execution.state = ExecutionState.COMPLETED
        execution.outcome = ExecutionOutcome.SUCCESS
        execution.termination_reason = TerminationReason.NONE
        execution.finish_time = datetime.now(timezone.utc)

        self._dispatch_queue.complete(scheduled_operation)
        self._emit_event(
            EventType.OPERATION_COMPLETED,
            scheduled_operation=scheduled_operation,
        )
        return scheduled_operation

    def fail_current(self) -> ScheduledOperation:
        return self._execute_state_mutation(self._fail_current_internal)

    def _fail_current_internal(self) -> ScheduledOperation:
        scheduled_operation = self._require_current_scheduled_operation()
        execution = self._require_execution(scheduled_operation.id)
        execution.state = ExecutionState.FAILED
        execution.outcome = ExecutionOutcome.FAILURE
        execution.termination_reason = TerminationReason.INTERNAL_ERROR
        execution.finish_time = datetime.now(timezone.utc)

        self._dispatch_queue.complete(scheduled_operation)
        self._emit_event(
            EventType.OPERATION_FAILED,
            scheduled_operation=scheduled_operation,
        )
        return scheduled_operation

    def pause_current(self) -> bool:
        return self._execute_state_mutation(self._pause_current_internal)

    def _pause_current_internal(self) -> bool:
        scheduled_operation = self._require_current_scheduled_operation()
        execution = self._require_execution(scheduled_operation.id)
        if execution.state is not ExecutionState.RUNNING:
            raise RuntimeError("current operation is not running")

        if not self._request_handler.request_operation_with_retry_sync(
            scheduled_operation,
            EventType.OPERATION_PAUSE_REQUESTED,
        ):
            return False

        self._is_paused = True
        execution.state = ExecutionState.PAUSED

        self._emit_event(
            EventType.OPERATION_MANAGER_PAUSED,
            scheduled_operation=scheduled_operation,
        )
        self._emit_event(
            EventType.OPERATION_PAUSED,
            scheduled_operation=scheduled_operation,
        )
        return True

    def request_resume_current(self) -> bool:
        return self._execute_state_mutation(self._request_resume_current_internal)

    def _request_resume_current_internal(self) -> bool:
        scheduled_operation = self._require_current_scheduled_operation()
        return self._request_handler.request_operation_with_retry_sync(
            scheduled_operation,
            EventType.OPERATION_RESUME_REQUESTED,
        )

    def cancel(self, operation_id: UUID) -> ScheduledOperation | None:
        return self._execute_state_mutation(lambda: self._cancel_internal(operation_id))

    def _cancel_internal(self, operation_id: UUID) -> ScheduledOperation | None:
        scheduled_operation = self.get_scheduled_operation(operation_id)
        if scheduled_operation is None:
            return None

        if not self._request_handler.request_operation_with_retry_sync(
            scheduled_operation,
            EventType.OPERATION_CANCEL_REQUESTED,
        ):
            return None

        cancelled_operation = self._dispatch_queue.cancel(operation_id)
        if cancelled_operation is None:
            return None

        execution = self._require_execution(cancelled_operation.id)
        execution.state = ExecutionState.CANCELLED
        execution.outcome = ExecutionOutcome.CANCELLED
        execution.termination_reason = TerminationReason.USER_REQUEST
        execution.finish_time = datetime.now(timezone.utc)

        self._request_handler.clear_request_retry_state(cancelled_operation.id)
        self._emit_event(
            EventType.OPERATION_CANCELLED,
            scheduled_operation=cancelled_operation,
        )
        return cancelled_operation

    def get_event_history(self, limit: int | None = None) -> list[DispatchEvent]:
        if limit is None:
            return list(self._event_history)
        return list(self._event_history[-limit:])

    def get_history_entries(
        self,
        limit: int | None = None,
    ) -> list[OperationHistoryEntry]:
        history_operations = self._dispatch_queue.history(limit=limit)
        history_entries: list[OperationHistoryEntry] = []

        for scheduled_operation in history_operations:
            operation_id = scheduled_operation.id
            execution = self._executions_by_operation_id.get(operation_id)
            if execution is None:
                continue

            operation_events = list(self._events_by_operation_id.get(operation_id, []))
            history_entries.append(
                OperationHistoryEntry(
                    scheduled_operation=scheduled_operation,
                    execution=execution,
                    events=operation_events,
                )
            )

        return history_entries

    def _require_current_scheduled_operation(self) -> ScheduledOperation:
        scheduled_operation = self.current_scheduled_operation
        if scheduled_operation is None:
            raise RuntimeError("no current operation")
        return scheduled_operation

    def get_scheduled_operation(self, operation_id: UUID) -> ScheduledOperation | None:
        return self._dispatch_queue.get(operation_id)

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

        next_operation = self._dispatch_queue.peek()
        if next_operation is None:
            return None

        now = datetime.now(timezone.utc)

        release_wait_seconds = 0.0
        if next_operation.release_date is not None:
            release_wait_seconds = (next_operation.release_date - now).total_seconds()
            if release_wait_seconds < 0:
                release_wait_seconds = 0.0

        cooldown_wait_seconds = self._request_handler.request_cooldown_wait_seconds(
            next_operation,
            now,
        )

        return max(release_wait_seconds, cooldown_wait_seconds)

    def _emit_event(
        self,
        event_type: EventType,
        scheduled_operation: ScheduledOperation | None = None,
        data: dict[str, Any] | None = None,
    ) -> DispatchEvent:
        resolved_payload = {} if data is None else dict(data)
        nil_uuid = UUID(int=0)
        operation_id = (
            scheduled_operation.id if scheduled_operation is not None else nil_uuid
        )
        execution = self._executions_by_operation_id.get(operation_id)
        execution_id = execution.id if execution is not None else nil_uuid

        event = DispatchEvent(
            execution_id=execution_id,
            operation_id=operation_id,
            event_type=event_type,
            payload=resolved_payload,
        )
        self._append_event_history(event)
        if operation_id.int != 0:
            self._append_operation_event(event.operation_id, event)
        self._log_event(event)
        self._notification_handler.notify(event)
        self._notify_wakeup()
        return event

    def _append_event_history(self, event: DispatchEvent) -> None:
        self._event_history.append(event)
        if len(self._event_history) > self._event_history_limit:
            self._event_history = self._event_history[-self._event_history_limit :]

    def _append_operation_event(self, operation_id: UUID, event: DispatchEvent) -> None:
        self._events_by_operation_id.setdefault(operation_id, []).append(event)

    def _log_event(self, event: DispatchEvent) -> None:
        if self._logger is None:
            return

        if event.event_type in {
            EventType.OPERATION_MANAGER_PAUSED,
            EventType.OPERATION_MANAGER_STOPPED,
        }:
            self._logger.warning("EVENT %s", event.event_type)
        elif event.event_type in {
            EventType.OPERATION_MANAGER_RESUMED,
        }:
            self._logger.info("EVENT %s", event.event_type)
        else:
            self._logger.debug(
                "EVENT %s for operation_id=%s",
                event.event_type,
                event.operation_id,
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

    def _require_execution(self, operation_id: UUID) -> OperationExecution:
        execution = self._executions_by_operation_id.get(operation_id)
        if execution is None:
            raise RuntimeError(f"missing execution state for operation {operation_id}")
        return execution

    def _set_current_execution_state(self, state: ExecutionState) -> None:
        current = self.current_scheduled_operation
        if current is None:
            return
        execution = self._executions_by_operation_id.get(current.id)
        if execution is None:
            return
        execution.state = state

    def _get_execution_id(self, operation_id: UUID) -> UUID | None:
        execution = self._executions_by_operation_id.get(operation_id)
        if execution is None:
            return None
        return execution.id
