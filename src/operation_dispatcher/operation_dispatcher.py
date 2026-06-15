from __future__ import annotations

import logging
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
        start_paused: bool = False,
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
                [
                    datetime | None,
                    datetime | None,
                    bool,
                    int | None,
                    History | None,
                ],
                History | None,
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
            is_paused=start_paused,
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
            emit_event=self._event_service.emit_event,
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
        await self._runtime_service.run(start_paused=self._state_store.is_paused)

    def request_stop(self) -> None:
        self._runtime_service.request_stop()

    # History

    def get_history(
        self,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        resolve_operations: bool = False,
        limit: int | None = None,
    ) -> History:
        """Get dispatcher history.

        Selection semantics:
        - If `from_time` and `to_time` are both `None`, `limit` selects the most
          recent events.
        - If a time window is provided, events are filtered chronologically and
          `limit` selects the most recent events within that filtered window.
        """
        return self._history_service.get_history(
            from_time=from_time,
            to_time=to_time,
            resolve_operations=resolve_operations,
            limit=limit,
        )

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
