from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from operation_dispatcher.models import (
    CompleteOperationCommand,
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

        if (
            operation.state is not ExecutionState.QUEUED
            or operation.outcome is not ExecutionOutcome.NONE
        ):
            raise ValueError("new operations must be QUEUED with outcome NONE")
        if operation.start_time is not None or operation.finish_time is not None:
            raise ValueError("new operations cannot contain execution timestamps")
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
            ).model_copy(deep=True)
        )

    def request_operation_completion(
        self,
        operation_id: UUID,
        outcome: ExecutionOutcome,
        termination_reason: TerminationReason | None = None,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        command = CompleteOperationCommand(
            outcome=outcome,
            termination_reason=termination_reason,
            meta_data={} if meta_data is None else deepcopy(meta_data),
        )
        assert command.termination_reason is not None  # Resolved by command validation.
        request_metadata = {
            **command.meta_data,
            "outcome": command.outcome.value,
            "termination_reason": command.termination_reason.value,
        }
        request_metadata.pop("request_decision", None)
        return self._request_transition(
            operation_id,
            EventType.OPERATION_COMPLETE_REQUESTED,
            self._require_completable_operation,
            lambda operation: (
                self._complete_operation_internal(operation.id, request_metadata)
                if command.outcome is ExecutionOutcome.SUCCESS
                else self._fail_operation_internal(
                    operation.id, command.termination_reason, request_metadata
                )
            ),
            request_metadata,
            use_retry=False,
            require_callback=True,
        )

    def _request_transition(
        self,
        operation_id: UUID,
        event_type: EventType,
        validate: Callable[[UUID], Operation | None],
        commit: Callable[[Operation], Operation],
        meta_data: dict[str, Any] | None,
        *,
        use_retry: bool = True,
        require_callback: bool = False,
    ) -> Operation | None:
        handler = self._state_store.request_handler
        if handler is None:
            return None
        metadata = deepcopy(meta_data)

        def prepare():
            operation = validate(operation_id)
            if operation is None or (
                use_retry and not handler.can_request(operation, event_type)
            ):
                return None
            return operation, operation.model_copy(deep=True)

        selected = self._mutation_service.execute(prepare)
        if selected is None:
            return None
        operation, snapshot = selected
        decision = handler.evaluate_operation_request_sync(
            snapshot, event_type, metadata, require_callback=require_callback
        )

        def finalize():
            if not handler.complete_request(
                snapshot, event_type, decision, metadata, use_retry=use_retry
            ):
                return None
            current = validate(operation_id)
            if (
                current is not operation
                or current.model_dump() != snapshot.model_dump()
            ):
                raise RuntimeError("operation changed during approval")
            return commit(operation).model_copy(deep=True)

        return self._mutation_service.execute(finalize)

    def _require_completable_operation(self, operation_id: UUID) -> Operation:
        if self.get_operation(operation_id) is None:
            raise LookupError("operation not found")
        operation = self._require_current_operation(operation_id)
        if operation.state not in (ExecutionState.RUNNING, ExecutionState.PAUSED):
            raise RuntimeError("operation must be running or paused")
        return operation

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
        self._state_store.request_handler.clear_request_retry_state(operation.id)
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
            ).model_copy(deep=True)
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
        self._state_store.request_handler.clear_request_retry_state(operation.id)
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
        meta_data = deepcopy(meta_data)

        def validate(operation_id):
            operation = self._require_current_operation(operation_id)
            if enforce_running_state and operation.state is not ExecutionState.RUNNING:
                raise RuntimeError("current operation is not running")
            return operation

        return (
            self._request_transition(
                operation_id,
                EventType.OPERATION_PAUSE_REQUESTED,
                validate,
                lambda operation: self._set_execution_state(
                    operation,
                    ExecutionState.PAUSED,
                    EventType.OPERATION_PAUSED,
                    meta_data,
                ),
                meta_data,
            )
            is not None
        )

    def resume_operation(
        self,
        operation_id: UUID,
        enforce_paused_state: bool = True,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        meta_data = deepcopy(meta_data)

        def validate(operation_id):
            operation = self._require_current_operation(operation_id)
            if enforce_paused_state and operation.state is not ExecutionState.PAUSED:
                raise RuntimeError("current operation is not paused")
            return operation

        return (
            self._request_transition(
                operation_id,
                EventType.OPERATION_RESUME_REQUESTED,
                validate,
                lambda operation: self._set_execution_state(
                    operation,
                    ExecutionState.RUNNING,
                    EventType.OPERATION_RESUMED,
                    meta_data,
                ),
                meta_data,
            )
            is not None
        )

    def _set_execution_state(self, operation, state, event_type, meta_data):
        old_operation = operation.model_copy(deep=True)
        self._transition_operation_state(operation, state=state)
        self._event_service.emit_event(
            event_type,
            operation=operation,
            meta_data=meta_data,
            old_operation=old_operation,
        )
        return operation

    def cancel(
        self,
        operation_id: UUID,
        termination_reason: TerminationReason = TerminationReason.INTERNAL_ERROR,
        meta_data: dict[str, Any] | None = None,
    ) -> Operation | None:
        meta_data = deepcopy(meta_data)

        def validate(operation_id):
            operation = self.get_operation(operation_id)
            if operation is None or operation.state is ExecutionState.COMPLETED:
                return None
            return operation

        def commit(operation):
            old_operation = operation.model_copy(deep=True)
            cancelled = self._state_store.dispatch_queue.cancel(operation.id)
            if cancelled is None:
                raise RuntimeError("operation changed during approval")
            self._transition_operation_state(
                operation,
                state=ExecutionState.COMPLETED,
                outcome=ExecutionOutcome.CANCELLED,
                termination_reason=termination_reason,
                set_finish_time=True,
            )
            self._state_store.request_handler.clear_request_retry_state(operation.id)
            self._event_service.emit_event(
                EventType.OPERATION_CANCELLED,
                operation=operation,
                meta_data=meta_data,
                old_operation=old_operation,
            )
            return operation

        return self._request_transition(
            operation_id,
            EventType.OPERATION_CANCEL_REQUESTED,
            validate,
            commit,
            meta_data,
        )

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

        if operation.state is ExecutionState.COMPLETED:
            raise RuntimeError("cannot update a completed operation")

        if not updates:
            return operation.model_copy(deep=True)

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
            setattr(operation, field, deepcopy(getattr(validated_operation, field)))

        self._state_store.dispatch_queue.resort()

        if old_operation.model_dump() != operation.model_dump():
            self._event_service.emit_event(
                EventType.OPERATION_UPDATED,
                operation=operation,
                meta_data=meta_data,
                old_operation=old_operation,
            )

        return operation.model_copy(deep=True)

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
