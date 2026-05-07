from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import cast
from typing import Any
from uuid import UUID
import logging

from .models import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    OperationManagerEvent,
    OperationManagerEventType,
    OperationManagerState,
    TerminationReason,
)
from .schedule import Schedule


class OperationManager:
    """
    A runtime component that manages the execution of operations based on a schedule.
    It supports adding operations, starting the next operation, completing or failing the current operation, and running continuously with a polling mechanism.
    The operation manager can be paused and resumed, and it tracks the state of the current operation and the overall schedule.
    """

    def __init__(
        self,
        agent_id: str,
        on_event_callback: Callable[[OperationManagerEvent], object] | None = None,
        poll_interval_seconds: float = 0.1,
        start_request_max_retries: int = 5,
        start_request_retry_cooldown_seconds: float = 1.0,
        payload_model: Any | None = None,
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

        self._logger = logger
        self._schedule = Schedule(agent_id=agent_id)
        self._on_event_callback = on_event_callback
        self._payload_model = payload_model
        self._start_request_max_retries = start_request_max_retries
        self._start_request_retry_cooldown_seconds = (
            start_request_retry_cooldown_seconds
        )
        self._start_request_denial_counts: dict[UUID, int] = {}
        self._start_request_cooldown_until: dict[UUID, datetime] = {}
        self._current_operation: Operation | None = None
        self._is_paused = False
        self._is_running = False
        self._stop_requested = False
        self._poll_interval_seconds = poll_interval_seconds
        self._running_since: datetime | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._wakeup_event: asyncio.Event | None = None
        self._event_listeners: list[Callable[[OperationManagerEvent], object]] = []
        self._event_history: list[OperationManagerEvent] = []
        self._event_history_limit = 1000

    @property
    def schedule(self) -> Schedule:
        return self._schedule

    @property
    def current_operation(self) -> Operation | None:
        return self._current_operation

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def payload_model(self) -> Any | None:
        return self._payload_model

    def add(self, operation: Operation) -> None:
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
        if self._is_running and running_since is not None:
            uptime_seconds = (now - running_since).total_seconds()

        return OperationManagerState(
            is_running=self._is_running,
            is_paused=self._is_paused,
            queue_size=len(self._schedule),
            current_operation=self._current_operation,
            running_since=running_since,
            uptime_seconds=uptime_seconds,
        )

    def pause(self) -> None:
        self._is_paused = True
        self._emit_event(OperationManagerEventType.OPERATION_MANAGER_PAUSED)

    def resume(self) -> None:
        self._clear_all_start_request_retry_state()
        self._is_paused = False
        if self._current_operation is not None:
            self._emit_event(
                OperationManagerEventType.OPERATION_RESUME_REQUESTED,
                operation=self._current_operation,
            )
        self._emit_event(OperationManagerEventType.OPERATION_MANAGER_RESUMED)

    def _start_next(self) -> Operation | None:
        if self._is_paused:
            return None

        if self._current_operation is not None:
            raise RuntimeError("an operation is already running")

        operation = self._schedule.next()
        if operation is None:
            return None

        self._current_operation = operation
        self._emit_event(
            OperationManagerEventType.OPERATION_STARTED,
            operation=operation,
        )
        return operation

    async def run_once(self) -> Operation | None:
        if self._is_paused or self._current_operation is not None:
            return None

        next_operation = self._schedule.peek()
        if next_operation is None:
            return None

        if next_operation.time_window is not None:
            now = datetime.now(timezone.utc)
            if next_operation.time_window.start > now:
                return None

        if self._is_start_request_cooldown_active(next_operation):
            return None

        if not await self._request_operation_start(next_operation):
            self._on_start_request_denied(next_operation)
            return None

        self._clear_start_request_retry_state(next_operation.id)

        if not await self._request_operation_start_dispatch(next_operation):
            return None

        operation = self._start_next()
        if operation is None:
            return None

        return operation

    async def run(self) -> None:
        if self._is_running:
            raise RuntimeError("operation manager is already running")

        self._is_running = True
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
            self._is_running = False
            self._running_since = None
            self._emit_event(OperationManagerEventType.OPERATION_MANAGER_STOPPED)
            self._runtime_loop = None
            self._wakeup_event = None

    def request_stop(self) -> None:
        self._stop_requested = True
        self._notify_wakeup()

    def complete_current(self) -> Operation:
        operation = self._require_current_operation()

        if operation.lifecycle_status is LifecycleStatus.RUNNING:
            self._schedule.complete(operation)
        elif operation.lifecycle_status is LifecycleStatus.FINISHED:
            self._set_operation_finish_if_needed(operation)

        self._current_operation = None
        self._emit_event(
            OperationManagerEventType.OPERATION_COMPLETED,
            operation=operation,
        )
        return operation

    def fail_current(self) -> Operation:
        operation = self._require_current_operation()
        operation.execution_outcome = ExecutionOutcome.FAILED
        operation.termination_reason = TerminationReason.NONE
        self._schedule.complete(operation)
        self._current_operation = None
        self._emit_event(
            OperationManagerEventType.OPERATION_FAILED,
            operation=operation,
        )
        return operation

    def stop_current(self) -> Operation:
        operation = self._require_current_operation()
        self._emit_event(
            OperationManagerEventType.OPERATION_STOP_REQUESTED,
            operation=operation,
        )
        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.STOPPED
        self._schedule.complete(operation)
        self._current_operation = None
        self._emit_event(
            OperationManagerEventType.OPERATION_STOPPED,
            operation=operation,
        )
        return operation

    def cancel(self, operation_id: UUID) -> Operation | None:
        if (
            self._current_operation is not None
            and self._current_operation.id == operation_id
        ):
            operation = self._require_current_operation()
            self._emit_event(
                OperationManagerEventType.OPERATION_CANCEL_REQUESTED,
                operation=operation,
            )
            operation.execution_outcome = ExecutionOutcome.NONE
            operation.termination_reason = TerminationReason.CANCELLED_DURING_RUN
            self._schedule.complete(operation)
            self._current_operation = None
            self._clear_start_request_retry_state(operation.id)
            self._emit_event(
                OperationManagerEventType.OPERATION_CANCELLED,
                operation=operation,
            )
            return operation

        operation = self.get_operation(operation_id)
        if operation is not None:
            self._emit_event(
                OperationManagerEventType.OPERATION_CANCEL_REQUESTED,
                operation=operation,
            )

        operation = self._schedule.cancel(operation_id)
        if operation is not None:
            self._clear_start_request_retry_state(operation.id)
            self._emit_event(
                OperationManagerEventType.OPERATION_CANCELLED,
                operation=operation,
            )
        return operation

    def add_event_listener(
        self, listener: Callable[[OperationManagerEvent], object]
    ) -> None:
        self._event_listeners.append(listener)

    def remove_event_listener(
        self,
        listener: Callable[[OperationManagerEvent], object],
    ) -> None:
        self._event_listeners = [
            existing_listener
            for existing_listener in self._event_listeners
            if existing_listener != listener
        ]

    def get_event_history(
        self, limit: int | None = None
    ) -> list[OperationManagerEvent]:
        if limit is None:
            return list(self._event_history)
        return list(self._event_history[-limit:])

    def _require_current_operation(self) -> Operation:
        if self._current_operation is None:
            raise RuntimeError("no current operation")
        return self._current_operation

    def get_operation(self, operation_id: UUID) -> Operation | None:
        return self._schedule.get(operation_id)

    def _on_start_request_denied(self, operation: Operation) -> None:
        denial_count = self._start_request_denial_counts.get(operation.id, 0) + 1
        self._start_request_denial_counts[operation.id] = denial_count
        max_retries_reached = denial_count >= self._start_request_max_retries

        if self._start_request_retry_cooldown_seconds > 0:
            self._start_request_cooldown_until[operation.id] = datetime.now(
                timezone.utc
            ) + timedelta(seconds=self._start_request_retry_cooldown_seconds)

        self._emit_event(
            OperationManagerEventType.OPERATION_START_DENIED,
            operation=operation,
            data={
                "retry_count": denial_count,
                "max_retries": self._start_request_max_retries,
                "cooldown_seconds": self._start_request_retry_cooldown_seconds,
            },
        )

        if max_retries_reached:
            self.pause()

    def _is_start_request_cooldown_active(self, operation: Operation) -> bool:
        cooldown_until = self._start_request_cooldown_until.get(operation.id)
        if cooldown_until is None:
            return False

        now = datetime.now(timezone.utc)
        if cooldown_until > now:
            return True

        self._start_request_cooldown_until.pop(operation.id, None)
        return False

    def _clear_start_request_retry_state(self, operation_id: UUID) -> None:
        self._start_request_denial_counts.pop(operation_id, None)
        self._start_request_cooldown_until.pop(operation_id, None)

    def _clear_all_start_request_retry_state(self) -> None:
        self._start_request_denial_counts.clear()
        self._start_request_cooldown_until.clear()

    async def _request_operation_start(self, operation: Operation) -> bool:
        return await self._request_operation_event(
            operation,
            OperationManagerEventType.OPERATION_START_REQUESTED,
        )

    async def _request_operation_start_dispatch(self, operation: Operation) -> bool:
        return await self._request_operation_event(
            operation,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        )

    async def _request_operation_event(
        self,
        operation: Operation,
        event_type: OperationManagerEventType,
    ) -> bool:
        event = OperationManagerEvent(
            event_type=event_type,
            agent_id=operation.agent_id,
            operation_id=operation.id,
            operation_name=operation.name,
            data={},
        )
        self._event_history.append(event)
        if len(self._event_history) > self._event_history_limit:
            self._event_history = self._event_history[-self._event_history_limit :]

        is_allowed = self._on_event_callback is None
        if self._on_event_callback is not None:
            try:
                callback_result = self._on_event_callback(event)
                if inspect.isawaitable(callback_result):
                    callback_result = await _as_awaitable(callback_result)
                is_allowed = callback_result is True
            except Exception:
                is_allowed = False

        for listener in list(self._event_listeners):
            try:
                listener_result = listener(event)
                if inspect.isawaitable(listener_result):
                    self._schedule_listener_awaitable(_as_awaitable(listener_result))
            except Exception:
                pass

        if self._logger is not None:
            self._logger.debug(
                f"EVENT {event.event_type} for operation {event.operation_name} ({event.operation_id})"
            )

        self._notify_wakeup()
        return is_allowed

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

        cooldown_wait_seconds = 0.0
        cooldown_until = self._start_request_cooldown_until.get(next_operation.id)
        if cooldown_until is not None:
            cooldown_wait_seconds = (cooldown_until - now).total_seconds()
            if cooldown_wait_seconds <= 0:
                self._start_request_cooldown_until.pop(next_operation.id, None)
                cooldown_wait_seconds = 0.0

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
        self._event_history.append(event)
        if len(self._event_history) > self._event_history_limit:
            self._event_history = self._event_history[-self._event_history_limit :]

        if self._on_event_callback is not None:
            try:
                callback_result = self._on_event_callback(event)
                if inspect.isawaitable(callback_result):
                    self._schedule_listener_awaitable(_as_awaitable(callback_result))
            except Exception:
                pass

        for listener in list(self._event_listeners):
            try:
                listener_result = listener(event)
                if inspect.isawaitable(listener_result):
                    self._schedule_listener_awaitable(_as_awaitable(listener_result))
            except Exception:
                pass
        if self._logger is not None:
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

        self._notify_wakeup()
        return event

    def _schedule_listener_awaitable(self, awaitable: Awaitable[object]) -> None:
        try:
            running_loop = asyncio.get_running_loop()
            running_loop.create_task(awaitable)
            return
        except RuntimeError:
            pass

        if self._runtime_loop is None:
            return

        if not self._runtime_loop.is_running():
            return

        self._runtime_loop.call_soon_threadsafe(
            self._runtime_loop.create_task,
            awaitable,
        )

    def _notify_wakeup(self) -> None:
        if not self._is_running:
            return
        if self._runtime_loop is None or self._wakeup_event is None:
            return

        self._runtime_loop.call_soon_threadsafe(self._wakeup_event.set)


def _as_awaitable(result: object) -> Awaitable[object]:
    return cast(Awaitable[object], result)
