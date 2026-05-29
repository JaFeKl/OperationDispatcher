from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import queue
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from operation_dispatcher.dispatch_queue import DispatchQueue, SortRule
from operation_dispatcher.models import (
    DispatchEvent,
    EventType,
    ExecutionOutcome,
    ExecutionState,
    History,
    HistoryRecord,
    OperationDispatcherState,
    Operation,
    TerminationReason,
)
from operation_dispatcher.notification_handler import NotificationHandler
from operation_dispatcher.request_handler import RequestHandler
from operation_dispatcher.retry_policy import RetryPolicy
from operation_dispatcher.services import (
    DispatcherEventService,
    DispatcherHistoryService,
    DispatcherMutationService,
    DispatcherRuntimeService,
    DispatcherStateStore,
    OperationLifecycleService,
)
from operation_dispatcher.utils import get_changes


@dataclass(frozen=True)
class _CommandResult:
    accepted: bool
    operation: Operation | None = None


class OperationDispatcherReference:
    """
    Core operation dispatcher class responsible for managing the operation schedule, execution state, and event dispatching.
    """

    _DEFAULT_UPDATABLE_FIELDS = frozenset(
        {
            "payload",
            "priority",
            "release_date",
            "planned_duration",
            "due_date",
            "dependencies",
            "state",
            "outcome",
            "termination_reason",
            "retry_count",
            "start_time",
            "finish_time",
        }
    )

    def __init__(
        self,
        resource_id: str,
        poll_interval_seconds: float = 0.1,
        start_request_max_retries: int = 5,
        start_request_retry_cooldown_seconds: float = 1.0,
        request_event_timeout_seconds: float = 5.0,
        default_planned_duration: int | None = None,
        updatable_fields: list[str] | None = None,
        dispatch_queue_sort_rules: list[SortRule] | None = None,
        on_request_callback: (
            Callable[[DispatchEvent], bool | dict[str, Any] | None] | None
        ) = None,
        on_notification_callback: Callable[[DispatchEvent], object] | None = None,
        on_history_callback: (
            Callable[
                [int | None, History],
                History | list[HistoryRecord] | None,
            ]
            | None
        ) = None,
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
        if default_planned_duration is not None and default_planned_duration <= 0:
            raise ValueError("default_planned_duration must be > 0")

        self._logger = logger
        self._dispatch_queue = DispatchQueue(
            resource_id=resource_id,
            sort_rules=dispatch_queue_sort_rules,
        )
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
        self._default_planned_duration = default_planned_duration
        self._updatable_fields = self._resolve_updatable_fields(updatable_fields)
        self._on_history_callback = on_history_callback

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
            emit_event=self._emit_event,
            log_event=self._log_event,
            notify_wakeup=self._notify_wakeup,
            pause=self.pause_dispatcher_runtime,
        )

    @property
    def dispatch_queue(self) -> DispatchQueue:
        return self._dispatch_queue

    @property
    def current_operation(self) -> Operation | None:
        return self._dispatch_queue.pulled_operation

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

    def add(
        self,
        operation: Operation,
        apply_default_planned_duration: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> None:
        self._execute_state_mutation(
            lambda: self._add_internal(
                operation,
                apply_default_planned_duration,
                meta_data,
            )
        )

    def _add_internal(
        self,
        operation: Operation,
        apply_default_planned_duration: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> None:
        self._apply_default_planned_duration(operation, apply_default_planned_duration)

        self._dispatch_queue.add(operation)
        self._events_by_operation_id.setdefault(operation.id, [])
        self._emit_event(
            EventType.OPERATION_ADDED,
            operation=operation,
            meta_data=meta_data,
        )

    def get_schedule(self) -> list[Operation]:
        return self._dispatch_queue.list()

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

    def pause_dispatcher_runtime(self) -> None:
        self._execute_state_mutation(self._pause_internal)

    def _pause_internal(self) -> None:
        self._is_paused = True
        self._set_current_operation_state(ExecutionState.PAUSED)
        self._emit_event(EventType.OPERATION_DISPATCHER_PAUSED)

    def resume_dispatcher_runtime(self) -> None:
        self._execute_state_mutation(self._resume_internal)

    def _resume_internal(self) -> None:
        self._request_handler.clear_all_request_retry_state()
        self._is_paused = False
        self._emit_event(EventType.OPERATION_DISPATCHER_RESUMED)

    def _start_next(self) -> Operation | None:
        if self._is_paused:
            return None

        if self.current_operation is not None:
            raise RuntimeError("an operation is already running")

        operation = self._dispatch_queue.next()
        if operation is None:
            return None

        old_operation = self._snapshot_operation(operation)
        self._transition_operation_state(
            operation,
            state=ExecutionState.RUNNING,
            set_start_time_if_missing=True,
        )

        self._emit_event(
            EventType.OPERATION_STARTED,
            operation=operation,
            old_operation=old_operation,
        )
        return operation

    async def run_once(self) -> Operation | None:
        if self._is_paused or self.current_operation is not None:
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
        self._emit_event(EventType.OPERATION_DISPATCHER_STARTED)
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
            self._emit_event(EventType.OPERATION_DISPATCHER_STOPPED)
            self._runtime_loop = None
            self._wakeup_event = None

    def request_stop(self) -> None:
        self._execute_state_mutation(self._request_stop_internal)

    def _request_stop_internal(self) -> None:
        self._stop_requested = True
        self._notify_wakeup()

    def complete_current(
        self,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._execute_state_mutation(
            lambda: self._complete_current_internal(meta_data=meta_data)
        )

    def _complete_current_internal(
        self,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        operation = self._require_current_operation()
        old_operation = self._snapshot_operation(operation)
        self._transition_operation_state(
            operation,
            state=ExecutionState.COMPLETED,
            outcome=ExecutionOutcome.SUCCESS,
            termination_reason=TerminationReason.NONE,
            set_finish_time=True,
        )

        self._dispatch_queue.complete(operation)
        self._emit_event(
            EventType.OPERATION_COMPLETED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return operation

    def fail_current(
        self,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._execute_state_mutation(
            lambda: self._fail_current_internal(
                termination_reason=termination_reason,
                meta_data=meta_data,
            )
        )

    def _fail_current_internal(
        self,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        operation = self._require_current_operation()
        old_operation = self._snapshot_operation(operation)
        self._transition_operation_state(
            operation,
            state=ExecutionState.FAILED,
            outcome=ExecutionOutcome.FAILURE,
            termination_reason=termination_reason,
            set_finish_time=True,
        )

        self._dispatch_queue.complete(operation)
        self._emit_event(
            EventType.OPERATION_FAILED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return operation

    def pause_current_operation(
        self,
        enforce_running_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        result: _CommandResult = self._execute_state_mutation(
            lambda: self._pause_current_internal(
                enforce_running_state,
                meta_data=meta_data,
            )
        )
        return result.accepted

    def _pause_current_internal(
        self,
        enforce_running_state: bool,
        meta_data: dict[str, Any] | None = None,
    ) -> _CommandResult:
        operation = self._require_current_operation()
        if enforce_running_state and operation.state is not ExecutionState.RUNNING:
            raise RuntimeError("current operation is not running")

        if not self._request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_PAUSE_REQUESTED,
            meta_data=meta_data,
        ):
            return _CommandResult(False, operation)

        old_operation = self._snapshot_operation(operation)
        self._transition_operation_state(
            operation,
            state=ExecutionState.PAUSED,
        )
        self._emit_event(
            EventType.OPERATION_PAUSED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return _CommandResult(True, operation)

    def resume_current_operation(
        self,
        enforce_paused_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        result: _CommandResult = self._execute_state_mutation(
            lambda: self._resume_current_internal(
                enforce_paused_state,
                meta_data=meta_data,
            )
        )
        return result.accepted

    def _resume_current_internal(
        self,
        enforce_paused_state: bool,
        meta_data: dict[str, Any] | None = None,
    ) -> _CommandResult:
        operation = self._require_current_operation()
        if enforce_paused_state and operation.state is not ExecutionState.PAUSED:
            raise RuntimeError("current operation is not paused")

        accepted = self._request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_RESUME_REQUESTED,
            meta_data=meta_data,
        )

        if not accepted:
            return _CommandResult(False, operation)

        old_operation = self._snapshot_operation(operation)
        self._transition_operation_state(
            operation,
            state=ExecutionState.RUNNING,
        )
        self._emit_event(
            EventType.OPERATION_RESUMED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return _CommandResult(True, operation)

    def cancel(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        result: _CommandResult = self._execute_state_mutation(
            lambda: self._cancel_internal(
                operation_id,
                termination_reason=termination_reason,
                meta_data=meta_data,
            )
        )
        if not result.accepted:
            return None
        return result.operation

    def _cancel_internal(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> _CommandResult:
        operation = self.get_operation(operation_id)
        if operation is None:
            return _CommandResult(False)

        if not self._request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_CANCEL_REQUESTED,
            meta_data=meta_data,
        ):
            return _CommandResult(False, operation)

        cancelled_operation = self._dispatch_queue.cancel(operation_id)
        if cancelled_operation is None:
            return _CommandResult(False, operation)

        old_operation = self._snapshot_operation(cancelled_operation)
        self._transition_operation_state(
            cancelled_operation,
            state=ExecutionState.CANCELLED,
            outcome=ExecutionOutcome.CANCELLED,
            termination_reason=termination_reason,
            set_finish_time=True,
        )

        self._request_handler.clear_request_retry_state(cancelled_operation.id)
        self._emit_event(
            EventType.OPERATION_CANCELLED,
            operation=cancelled_operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return _CommandResult(True, cancelled_operation)

    def update(
        self,
        operation_id: UUID,
        updates: dict[str, Any],
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        return self._execute_state_mutation(
            lambda: self._update_internal(
                operation_id,
                updates,
                meta_data=meta_data,
            )
        )

    def _update_internal(
        self,
        operation_id: UUID,
        updates: dict[str, Any],
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        operation = self.get_operation(operation_id)
        if operation is None:
            return None

        if not updates:
            return operation

        if (
            self.current_operation is not None
            and self.current_operation.id == operation_id
            and operation.state is ExecutionState.RUNNING
        ):
            raise RuntimeError("cannot update a running operation")

        invalid_fields = sorted(set(updates) - self._updatable_fields)
        if invalid_fields:
            raise ValueError(f"unsupported operation update fields: {invalid_fields}")

        candidate_data = operation.model_dump()
        candidate_data.update(updates)
        validated_operation = Operation.model_validate(candidate_data)

        old_operation = self._snapshot_operation(operation)
        for field in updates:
            new_value = getattr(validated_operation, field)
            setattr(operation, field, new_value)

        if old_operation.model_dump() != operation.model_dump():
            self._emit_event(
                EventType.OPERATION_UPDATED,
                operation=operation,
                meta_data=meta_data,
                old_operation=old_operation,
            )

        return operation

    def get_event_history(self, limit: int | None = None) -> list[DispatchEvent]:
        if limit is None:
            return list(self._event_history)
        return list(self._event_history[-limit:])

    def get_history(
        self,
        limit: int | None = None,
    ) -> History:
        in_memory_history = self._get_in_memory_history(limit)
        if self._on_history_callback is None:
            return in_memory_history

        callback_result = self._on_history_callback(limit, in_memory_history)
        if callback_result is None:
            return in_memory_history
        if isinstance(callback_result, History):
            return callback_result

        return History(
            num_records=len(callback_result),
            records=callback_result,
        )

    def _get_in_memory_history(
        self,
        limit: int | None,
    ) -> History:
        history_operations = self._dispatch_queue.history(limit=limit)
        history_records: list[HistoryRecord] = []

        for operation in history_operations:
            operation_id = operation.id
            operation_events = list(self._events_by_operation_id.get(operation_id, []))
            history_records.append(
                HistoryRecord(
                    operation=operation,
                    events=operation_events,
                )
            )

        return History(
            num_records=len(history_records),
            records=history_records,
        )

    def _require_current_operation(self) -> Operation:
        operation = self.current_operation
        if operation is None:
            raise RuntimeError("no current operation")
        return operation

    def get_operation(self, operation_id: UUID) -> Operation | None:
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
        operation: Operation | None = None,
        meta_data: dict[str, Any] | None = None,
        old_operation: Operation | None = None,
    ) -> DispatchEvent:
        resolved_meta_data = {} if meta_data is None else dict(meta_data)
        resolved_changes = (
            get_changes(old_operation, operation)
            if old_operation is not None and operation is not None
            else []
        )

        operation_id = operation.id if operation is not None else None

        event = DispatchEvent(
            operation_id=operation_id,
            event_type=event_type,
            changes=resolved_changes,
            meta_data=resolved_meta_data,
        )
        self._append_event_history(event)
        if operation_id is not None:
            self._append_operation_event(operation_id, event)
        self._log_event(event)
        self._notification_handler.notify(event)
        self._notify_wakeup()
        return event

    def _snapshot_operation(self, operation: Operation) -> Operation:
        return operation.model_copy(deep=True)

    def _apply_default_planned_duration(
        self,
        operation: Operation,
        apply_default_planned_duration: bool,
    ) -> None:
        if (
            apply_default_planned_duration
            and operation.planned_duration is None
            and self._default_planned_duration is not None
        ):
            operation.planned_duration = self._default_planned_duration

    @classmethod
    def _resolve_updatable_fields(
        cls,
        updatable_fields: list[str] | None,
    ) -> frozenset[str]:
        if updatable_fields is None:
            return cls._DEFAULT_UPDATABLE_FIELDS

        resolved_fields = frozenset(updatable_fields)
        unknown_fields = sorted(resolved_fields - cls._DEFAULT_UPDATABLE_FIELDS)
        if unknown_fields:
            raise ValueError(
                f"updatable_fields contains unsupported fields: {unknown_fields}"
            )
        return resolved_fields

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
            EventType.OPERATION_DISPATCHER_PAUSED,
            EventType.OPERATION_DISPATCHER_STOPPED,
        }:
            self._logger.warning("EVENT %s", event.event_type)
        elif event.event_type in {
            EventType.OPERATION_DISPATCHER_RESUMED,
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

    def _transition_operation_state(
        self,
        operation: Operation,
        *,
        state: ExecutionState,
        outcome: ExecutionOutcome | None = None,
        termination_reason: TerminationReason | None = None,
        set_finish_time: bool = False,
        set_start_time_if_missing: bool = False,
    ) -> Operation:
        operation.state = state
        if outcome is not None:
            operation.outcome = outcome
        if termination_reason is not None:
            operation.termination_reason = termination_reason
        if set_start_time_if_missing and operation.start_time is None:
            operation.start_time = datetime.now(timezone.utc)
        if set_finish_time:
            operation.finish_time = datetime.now(timezone.utc)
        return operation

    def _set_current_operation_state(self, state: ExecutionState) -> None:
        current = self.current_operation
        if current is None:
            return
        current.state = state


class OperationDispatcher:

    _DEFAULT_UPDATABLE_FIELDS = frozenset(
        {
            "payload",
            "priority",
            "release_date",
            "planned_duration",
            "due_date",
            "dependencies",
            "state",
            "outcome",
            "termination_reason",
            "retry_count",
            "start_time",
            "finish_time",
        }
    )

    def __init__(
        self,
        resource_id: str,
        poll_interval_seconds: float = 0.1,
        start_request_max_retries: int = 5,
        start_request_retry_cooldown_seconds: float = 1.0,
        request_event_timeout_seconds: float = 5.0,
        default_planned_duration: int | None = None,
        updatable_fields: list[str] | None = None,
        dispatch_queue_sort_rules: list[SortRule] | None = None,
        on_request_callback: (
            Callable[[DispatchEvent], bool | dict[str, Any] | None] | None
        ) = None,
        on_notification_callback: Callable[[DispatchEvent], object] | None = None,
        on_history_callback: (
            Callable[
                [int | None, History],
                History | list[HistoryRecord] | None,
            ]
            | None
        ) = None,
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
        if default_planned_duration is not None and default_planned_duration <= 0:
            raise ValueError("default_planned_duration must be > 0")

        dispatch_queue = DispatchQueue(
            resource_id=resource_id,
            sort_rules=dispatch_queue_sort_rules,
        )
        updatable_fields_set = self._resolve_updatable_fields(updatable_fields)

        self._state_store = DispatcherStateStore(
            dispatch_queue=dispatch_queue,
            logger=logger,
            poll_interval_seconds=poll_interval_seconds,
            default_planned_duration=default_planned_duration,
            updatable_fields=updatable_fields_set,
            on_history_callback=on_history_callback,
        )

        self._mutation_service = DispatcherMutationService(self._state_store)
        self._event_service = DispatcherEventService(
            self._state_store,
            notify_wakeup=lambda: self._runtime_service.notify_wakeup(),
        )
        self._runtime_service = DispatcherRuntimeService(
            self._state_store,
            self._mutation_service,
            self._event_service,
        )
        self._operation_service = OperationLifecycleService(
            self._state_store,
            self._mutation_service,
            self._event_service,
        )
        self._history_service = DispatcherHistoryService(self._state_store)

        request_retry_policy = RetryPolicy(
            max_retries=start_request_max_retries,
            retry_cooldown_seconds=start_request_retry_cooldown_seconds,
        )

        self._state_store.notification_handler = NotificationHandler(
            on_notification_callback=on_notification_callback,
            runtime_loop_getter=lambda: self._state_store.runtime_loop,
            logger=logger,
        )
        self._state_store.request_handler = RequestHandler(
            on_request_callback=on_request_callback,
            request_retry_policy=request_retry_policy,
            request_event_timeout_seconds=request_event_timeout_seconds,
            append_event_history=self._event_service.append_event_history,
            append_operation_event=self._event_service.append_operation_event,
            emit_event=self._event_service.emit_event,
            log_event=self._event_service.log_event,
            notify_wakeup=self._runtime_service.notify_wakeup,
            pause=self._runtime_service.pause_dispatcher_runtime,
        )

    @property
    def dispatch_queue(self) -> DispatchQueue:
        return self._operation_service.dispatch_queue

    @property
    def current_operation(self) -> Operation | None:
        return self._operation_service.current_operation

    @property
    def is_paused(self) -> bool:
        return self._runtime_service.is_paused

    @property
    def is_running(self) -> bool:
        return self._runtime_service.is_running

    def get_operation(self, operation_id: UUID) -> Operation | None:
        return self._operation_service.get_operation(operation_id)

    # Operation lifecycle methods

    def add_operation(
        self,
        operation: Operation,
        apply_default_planned_duration: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> None:
        self._operation_service.add(
            operation=operation,
            apply_default_planned_duration=apply_default_planned_duration,
            meta_data=meta_data,
        )

    def update_operation(
        self,
        operation_id: UUID,
        updates: dict[str, Any],
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        return self._operation_service.update(
            operation_id,
            updates,
            meta_data=meta_data,
        )

    def cancel_operation(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        return self._operation_service.cancel(
            operation_id,
            termination_reason=termination_reason,
            meta_data=meta_data,
        )

    def pause_operation(
        self,
        operation_id: UUID,
        enforce_running_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        return self._operation_service.pause_operation(
            operation_id=operation_id,
            enforce_running_state=enforce_running_state,
            meta_data=meta_data,
        )

    def resume_operation(
        self,
        operation_id: UUID,
        enforce_paused_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        return self._operation_service.resume_operation(
            operation_id=operation_id,
            enforce_paused_state=enforce_paused_state,
            meta_data=meta_data,
        )

    def complete_operation(
        self,
        operation_id: UUID,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._operation_service.complete_operation(
            operation_id,
            meta_data=meta_data,
        )

    def fail_operation(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._operation_service.fail_operation(
            operation_id,
            termination_reason=termination_reason,
            meta_data=meta_data,
        )

    def get_schedule(self) -> list[Operation]:
        return self._operation_service.get_schedule()

    # Dispatcher runtime methods

    def get_state(self) -> OperationDispatcherState:
        return self._runtime_service.get_state()

    def pause_dispatcher_runtime(self) -> None:
        self._runtime_service.pause_dispatcher_runtime()

    def resume_dispatcher_runtime(self) -> None:
        self._runtime_service.resume_dispatcher_runtime()

    async def step_dispatch(self) -> Operation | None:
        return await self._runtime_service.step_dispatch()

    async def run(self) -> None:
        await self._runtime_service.run()

    def request_stop(self) -> None:
        self._runtime_service.request_stop()

    # History

    def get_event_history(self, limit: int | None = None) -> list[DispatchEvent]:
        return self._history_service.get_event_history(limit=limit)

    def get_history(
        self,
        limit: int | None = None,
    ) -> History:
        return self._history_service.get_history(limit=limit)

    @classmethod
    def _resolve_updatable_fields(
        cls,
        updatable_fields: list[str] | None,
    ) -> frozenset[str]:
        if updatable_fields is None:
            return cls._DEFAULT_UPDATABLE_FIELDS

        resolved_fields = frozenset(updatable_fields)
        unknown_fields = sorted(resolved_fields - cls._DEFAULT_UPDATABLE_FIELDS)
        if unknown_fields:
            raise ValueError(
                f"updatable_fields contains unsupported fields: {unknown_fields}"
            )
        return resolved_fields
