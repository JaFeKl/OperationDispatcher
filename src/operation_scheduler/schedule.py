from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from uuid import UUID

from .models import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    TerminationReason,
)


class Schedule:
    def __init__(
        self,
        agent_id: str,
        operations: Iterable[Operation] | None = None,
    ) -> None:
        self._agent_id = agent_id
        self._requires_time_window: bool | None = None
        self._operations: list[Operation] = []
        self._pulled_operations: list[Operation] = []
        self._completed_operations: list[Operation] = []

        for operation in operations or []:
            self.add(operation)

    def __len__(self) -> int:
        return len(self._operations)

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def pulled_operations(self) -> list[Operation]:
        return list(self._pulled_operations)

    @property
    def completed_operations(self) -> list[Operation]:
        return list(self._completed_operations)

    def history(self, limit: int | None = None) -> list[Operation]:
        operations = list(reversed(self._completed_operations))
        if limit is None:
            return operations
        return operations[:limit]

    def add(self, operation: Operation) -> None:
        self._validate_operation(operation)
        self._operations.append(operation)
        self._sort_operations()

    def next(self) -> Operation | None:
        if not self._operations:
            return None
        operation = self._operations.pop(0)
        operation.lifecycle_status = LifecycleStatus.RUNNING
        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.NONE
        if operation.start_time is None:
            operation.start_time = datetime.now(timezone.utc)
        self._pulled_operations.append(operation)
        return operation

    def peek(self) -> Operation | None:
        if not self._operations:
            return None
        return self._operations[0]

    def complete(self, operation: Operation) -> None:
        self._validate_operation(operation)

        if operation not in self._pulled_operations:
            raise ValueError(
                "operation must be pulled from this schedule before completion"
            )

        operation.lifecycle_status = LifecycleStatus.FINISHED
        if (
            operation.execution_outcome is ExecutionOutcome.NONE
            and operation.termination_reason is TerminationReason.NONE
        ):
            operation.execution_outcome = ExecutionOutcome.SUCCEEDED
        if operation.finish_time is None:
            operation.finish_time = datetime.now(timezone.utc)

        if operation not in self._completed_operations:
            self._completed_operations.append(operation)

    def list(self) -> list[Operation]:
        return list(self._operations)

    def clear(self) -> None:
        self._operations.clear()

    def clear_history(self) -> None:
        self._pulled_operations.clear()
        self._completed_operations.clear()

    def remove(self, operation_id: UUID) -> Operation | None:
        for index, operation in enumerate(self._operations):
            if operation.id == operation_id:
                return self._operations.pop(index)
        return None

    def cancel(self, operation_id: UUID) -> Operation | None:
        operation = self.remove(operation_id)
        if operation is None:
            return None
        operation.lifecycle_status = LifecycleStatus.FINISHED
        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.CANCELLED_BEFORE_START
        return operation

    def _sort_operations(self) -> None:
        max_datetime = datetime.max.replace(tzinfo=timezone.utc)
        self._operations.sort(
            key=lambda op: (
                op.time_window.start if op.time_window is not None else max_datetime,
                -op.priority,
            ),
        )

    def _validate_operation(self, operation: Operation) -> None:
        if self._agent_id is not None and operation.agent_id != self._agent_id:
            raise ValueError(
                "operation agent_id does not match schedule agent_id",
            )

        has_time_window = operation.time_window is not None
        if self._requires_time_window is None:
            self._requires_time_window = has_time_window
            return

        if has_time_window != self._requires_time_window:
            expected = (
                "with time_window"
                if self._requires_time_window
                else "without time_window"
            )
            raise ValueError(
                f"operation does not match queue type; expected operations {expected}",
            )
