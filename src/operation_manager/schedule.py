from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from .models import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    TerminationReason,
)


class ScheduleSortStrategy(str, Enum):
    START_TIME_THEN_PRIORITY = "start_time_then_priority"
    PRIORITY_THEN_START_TIME = "priority_then_start_time"


class Schedule:
    """Priority schedule for one agent with at most one active operation.

    The schedule keeps operations in three lifecycle buckets:
    - queued (`_operations`)
    - active (`_pulled_operation`)
    - completed history (`_completed_operations`)

    Invariants:
    - Only one operation can be active at a time.
    - One operation can only be in one bucket at a time.
    - Queue type is fixed after first insertion: all operations are either
      plain (no `time_window`) or windowed (with `time_window`).
    """

    def __init__(
        self,
        agent_id: str,
        operations: Iterable[Operation] | None = None,
        sort_strategy: ScheduleSortStrategy = ScheduleSortStrategy.START_TIME_THEN_PRIORITY,
    ) -> None:
        self._agent_id = agent_id
        self._sort_strategy = sort_strategy
        self._requires_time_window: bool | None = None
        self._operations: list[Operation] = []
        self._pulled_operation: Operation | None = None
        self._completed_operations: list[Operation] = []

        for operation in operations or []:
            self.add(operation)

    def __len__(self) -> int:
        return len(self._operations)

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def pulled_operation(self) -> Operation | None:
        return self._pulled_operation

    @property
    def completed_operations(self) -> list[Operation]:
        return list(self._completed_operations)

    @property
    def sort_strategy(self) -> ScheduleSortStrategy:
        return self._sort_strategy

    def history(self, limit: int | None = None) -> list[Operation]:
        operations = list(reversed(self._completed_operations))
        if limit is None:
            return operations
        return operations[:limit]

    def add(self, operation: Operation) -> None:
        self._validate_operation(operation)
        self._operations.append(operation)
        self._sort_operations()

    def get(self, operation_id: UUID) -> Operation | None:
        """
        Get an operation by ID from the schedule, including pending, pulled and completed operations.
        """
        if self._pulled_operation is not None:
            if self._pulled_operation.id == operation_id:
                return self._pulled_operation
        for operation in self._operations:
            if operation.id == operation_id:
                return operation
        for operation in self._completed_operations:
            if operation.id == operation_id:
                return operation
        return None

    def next(self) -> Operation | None:
        if self._pulled_operation is not None:
            raise RuntimeError(
                "cannot pull next operation while another operation is active"
            )

        if not self._operations:
            return None

        operation = self._operations.pop(0)
        operation.lifecycle_status = LifecycleStatus.RUNNING
        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.NONE
        if operation.start_time is None:
            operation.start_time = datetime.now(timezone.utc)
        self._pulled_operation = operation
        return operation

    def peek(self) -> Operation | None:
        if not self._operations:
            return None
        return self._operations[0]

    def complete(self, operation: Operation) -> None:
        self._validate_operation(operation)

        if self._pulled_operation is None or self._pulled_operation is not operation:
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

        self._archive_completed_operation(operation)
        self._pulled_operation = None

    def list(self) -> list[Operation]:
        return list(self._operations)

    def clear(self) -> None:
        self._operations.clear()

    def clear_history(self) -> None:
        self._completed_operations.clear()

    def remove(self, operation_id: UUID) -> Operation | None:
        for index, operation in enumerate(self._operations):
            if operation.id == operation_id:
                return self._operations.pop(index)
        return None

    def cancel(self, operation_id: UUID) -> Operation | None:
        if (
            self._pulled_operation is not None
            and self._pulled_operation.id == operation_id
        ):
            operation = self._pulled_operation
            operation.lifecycle_status = LifecycleStatus.FINISHED
            operation.execution_outcome = ExecutionOutcome.NONE
            operation.termination_reason = TerminationReason.CANCELLED_DURING_RUN
            if operation.finish_time is None:
                operation.finish_time = datetime.now(timezone.utc)
            self._archive_completed_operation(operation)
            self._pulled_operation = None
            return operation

        operation = self.remove(operation_id)
        if operation is None:
            return None

        operation.lifecycle_status = LifecycleStatus.FINISHED
        operation.execution_outcome = ExecutionOutcome.NONE
        operation.termination_reason = TerminationReason.CANCELLED_BEFORE_START
        if operation.finish_time is None:
            operation.finish_time = datetime.now(timezone.utc)
        self._archive_completed_operation(operation)
        return operation

    def _archive_completed_operation(self, operation: Operation) -> None:
        if operation not in self._completed_operations:
            self._completed_operations.append(operation)

    def _sort_operations(self) -> None:
        max_datetime = datetime.max.replace(tzinfo=timezone.utc)

        if self._sort_strategy is ScheduleSortStrategy.PRIORITY_THEN_START_TIME:
            self._operations.sort(
                key=lambda op: (
                    -op.priority,
                    (
                        op.time_window.start
                        if op.time_window is not None
                        else max_datetime
                    ),
                ),
            )
            return

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
