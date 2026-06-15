from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from operation_dispatcher.models import (
    EventType,
    ExecutionOutcome,
    ExecutionState,
    Operation,
    TerminationReason,
)

from .event_service import DispatcherEventService
from .mutation_service import DispatcherMutationService
from .state_store import DispatcherStateStore


class OperationLifecycleService:
    def __init__(
        self,
        state_store: DispatcherStateStore,
        mutation_service: DispatcherMutationService,
        event_service: DispatcherEventService,
    ) -> None:
        self._state_store = state_store
        self._mutation_service = mutation_service
        self._event_service = event_service

    @property
    def dispatch_queue(self):
        return self._state_store.dispatch_queue

    @property
    def current_operation(self) -> Operation | None:
        return self._state_store.dispatch_queue.pulled_operation

    def add(
        self,
        operation: Operation,
        apply_default_planned_duration: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> None:
        self._mutation_service.execute(
            lambda: self._add_internal(
                operation,
                apply_default_planned_duration,
                meta_data=meta_data,
            )
        )

    def _add_internal(
        self,
        operation: Operation,
        apply_default_planned_duration: bool,
        meta_data: dict[str, Any] | None,
    ) -> None:
        if (
            apply_default_planned_duration
            and operation.planned_duration is None
            and self._state_store.default_planned_duration is not None
        ):
            operation.planned_duration = self._state_store.default_planned_duration

        self._state_store.dispatch_queue.add(operation)
        self._event_service.emit_event(
            EventType.OPERATION_ADDED,
            operation=operation,
            meta_data=meta_data,
        )

    def get_schedule(self) -> list[Operation]:
        return self._state_store.dispatch_queue.list()

    def get_operation(self, operation_id: UUID) -> Operation | None:
        return self._state_store.dispatch_queue.get(operation_id)

    def complete_operation(
        self,
        operation_id: UUID,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._mutation_service.execute(
            lambda: self._complete_operation_internal(
                operation_id,
                meta_data=meta_data,
            )
        )

    def _complete_operation_internal(
        self,
        operation_id: UUID,
        meta_data: dict[str, Any] | None,
    ) -> Operation:
        operation = self._require_current_operation(operation_id)
        old_operation = operation.model_copy(deep=True)
        self._transition_operation_state(
            operation,
            state=ExecutionState.COMPLETED,
            outcome=ExecutionOutcome.SUCCESS,
            termination_reason=TerminationReason.NONE,
            set_finish_time=True,
        )
        self._state_store.dispatch_queue.complete(operation)
        self._event_service.emit_event(
            EventType.OPERATION_COMPLETED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return operation

    def fail_operation(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation:
        return self._mutation_service.execute(
            lambda: self._fail_operation_internal(
                operation_id,
                termination_reason=termination_reason,
                meta_data=meta_data,
            )
        )

    def _fail_operation_internal(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason,
        meta_data: dict[str, Any] | None,
    ) -> Operation:
        operation = self._require_current_operation(operation_id)
        old_operation = operation.model_copy(deep=True)
        self._transition_operation_state(
            operation,
            state=ExecutionState.COMPLETED,
            outcome=ExecutionOutcome.FAILURE,
            termination_reason=termination_reason,
            set_finish_time=True,
        )
        self._state_store.dispatch_queue.complete(operation)
        self._event_service.emit_event(
            EventType.OPERATION_FAILED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return operation

    def pause_operation(
        self,
        operation_id: UUID,
        enforce_running_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        return self._mutation_service.execute(
            lambda: self._pause_operation_internal(
                operation_id,
                enforce_running_state,
                meta_data=meta_data,
            )
        )

    def _pause_operation_internal(
        self,
        operation_id: UUID,
        enforce_running_state: bool,
        meta_data: dict[str, Any] | None,
    ) -> bool:
        operation = self._require_current_operation(operation_id)
        if enforce_running_state and operation.state is not ExecutionState.RUNNING:
            raise RuntimeError("current operation is not running")

        request_handler = self._state_store.request_handler
        if request_handler is None:
            return False

        if not request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_PAUSE_REQUESTED,
            meta_data=meta_data,
        ):
            return False

        old_operation = operation.model_copy(deep=True)
        self._transition_operation_state(operation, state=ExecutionState.PAUSED)
        self._event_service.emit_event(
            EventType.OPERATION_PAUSED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return True

    def resume_operation(
        self,
        operation_id: UUID,
        enforce_paused_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        return self._mutation_service.execute(
            lambda: self._resume_operation_internal(
                operation_id,
                enforce_paused_state,
                meta_data=meta_data,
            )
        )

    def _resume_operation_internal(
        self,
        operation_id: UUID,
        enforce_paused_state: bool,
        meta_data: dict[str, Any] | None,
    ) -> bool:
        operation = self._require_current_operation(operation_id)
        if enforce_paused_state and operation.state is not ExecutionState.PAUSED:
            raise RuntimeError("current operation is not paused")

        request_handler = self._state_store.request_handler
        if request_handler is None:
            return False

        accepted = request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_RESUME_REQUESTED,
            meta_data=meta_data,
        )
        if not accepted:
            return False

        old_operation = operation.model_copy(deep=True)
        self._transition_operation_state(operation, state=ExecutionState.RUNNING)
        self._event_service.emit_event(
            EventType.OPERATION_RESUMED,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return True

    def cancel(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        return self._mutation_service.execute(
            lambda: self._cancel_internal(
                operation_id,
                termination_reason=termination_reason,
                meta_data=meta_data,
            )
        )

    def _cancel_internal(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason,
        meta_data: dict[str, Any] | None,
    ) -> Operation | None:
        operation = self.get_operation(operation_id)
        if operation is None:
            return None

        request_handler = self._state_store.request_handler
        if request_handler is None:
            return None

        if not request_handler.request_operation_with_retry_sync(
            operation,
            EventType.OPERATION_CANCEL_REQUESTED,
            meta_data=meta_data,
        ):
            return None

        cancelled_operation = self._state_store.dispatch_queue.cancel(operation_id)
        if cancelled_operation is None:
            return None

        old_operation = cancelled_operation.model_copy(deep=True)
        self._transition_operation_state(
            cancelled_operation,
            state=ExecutionState.COMPLETED,
            outcome=ExecutionOutcome.CANCELLED,
            termination_reason=termination_reason,
            set_finish_time=True,
        )

        request_handler.clear_request_retry_state(cancelled_operation.id)
        self._event_service.emit_event(
            EventType.OPERATION_CANCELLED,
            operation=cancelled_operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return cancelled_operation

    def update(
        self,
        operation_id: UUID,
        updates: dict[str, Any],
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        return self._mutation_service.execute(
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
        meta_data: dict[str, Any] | None,
    ) -> Operation | None:
        operation = self.get_operation(operation_id)
        if operation is None:
            return None

        if not updates:
            return operation

        current = self.current_operation
        if (
            current is not None
            and current.id == operation_id
            and operation.state is ExecutionState.RUNNING
        ):
            raise RuntimeError("cannot update a running operation")

        invalid_fields = sorted(set(updates) - self._state_store.updatable_fields)
        if invalid_fields:
            raise ValueError(f"unsupported operation update fields: {invalid_fields}")

        candidate_data = operation.model_dump()
        candidate_data.update(updates)
        validated_operation = Operation.model_validate(candidate_data)

        old_operation = operation.model_copy(deep=True)
        for field in updates:
            setattr(operation, field, getattr(validated_operation, field))

        self._state_store.dispatch_queue.resort()

        if old_operation.model_dump() != operation.model_dump():
            self._event_service.emit_event(
                EventType.OPERATION_UPDATED,
                operation=operation,
                meta_data=meta_data,
                old_operation=old_operation,
            )

        return operation

    def _require_current_operation(self, operation_id: UUID) -> Operation:
        current = self.current_operation
        if current is None:
            raise RuntimeError("no current operation")
        if current.id != operation_id:
            raise RuntimeError("operation is not current")
        return current

    @staticmethod
    def _transition_operation_state(
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
