from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone

from .models import Operation, OperationStatus, TimedOperation


class Schedule:
    def __init__(
        self,
        operations: Iterable[Operation] | None = None,
        operation_class: type[Operation] = Operation,
        agent_id: str | None = None,
    ) -> None:
        self._operation_class = operation_class
        self._agent_id = agent_id
        self._is_timed_mode = _is_timed_operation_class(operation_class)
        self._operations: list[Operation] = []
        self._pulled_operations: list[Operation] = []
        self._completed_operations: list[Operation] = []

        for operation in operations or []:
            self.add(operation)

    def __len__(self) -> int:
        return len(self._operations)

    @property
    def agent_id(self) -> str | None:
        return self._agent_id

    @property
    def pulled_operations(self) -> list[Operation]:
        return list(self._pulled_operations)

    @property
    def completed_operations(self) -> list[Operation]:
        return list(self._completed_operations)

    def add(self, operation: Operation) -> None:
        self._validate_operation(operation)
        self._operations.append(operation)
        self._sort_operations()

    def next(self) -> Operation | None:
        if not self._operations:
            return None
        operation = self._operations.pop(0)
        operation.status = OperationStatus.RUNNING
        if (
            isinstance(operation, TimedOperation)
            and operation.actual_start_time is None
        ):
            operation.actual_start_time = datetime.now(timezone.utc)
        self._pulled_operations.append(operation)
        return operation

    def complete(self, operation: Operation) -> None:
        self._validate_operation(operation)

        if operation not in self._pulled_operations:
            raise ValueError(
                "operation must be pulled from this schedule before completion"
            )

        operation.status = OperationStatus.COMPLETED
        if (
            isinstance(operation, TimedOperation)
            and operation.actual_finish_time is None
        ):
            operation.actual_finish_time = datetime.now(timezone.utc)

        if operation not in self._completed_operations:
            self._completed_operations.append(operation)

    def list(self) -> list[Operation]:
        return list(self._operations)

    def clear(self) -> None:
        self._operations.clear()

    def clear_history(self) -> None:
        self._pulled_operations.clear()
        self._completed_operations.clear()

    def _sort_operations(self) -> None:
        if self._is_timed_mode:
            self._operations.sort(
                key=lambda op: (
                    _get_datetime_field(op, "planned_start_time"),
                    -op.priority,
                ),
            )
            return
        self._operations.sort(key=lambda op: op.priority, reverse=True)

    def _validate_operation(self, operation: Operation) -> None:
        if not isinstance(operation, self._operation_class):
            raise TypeError(
                "operation must be an instance of " f"{self._operation_class.__name__}",
            )
        if self._agent_id is not None and operation.agent_id != self._agent_id:
            raise ValueError(
                "operation agent_id does not match schedule agent_id",
            )


def _is_timed_operation_class(operation_class: type[Operation]) -> bool:
    if issubclass(operation_class, TimedOperation):
        return True
    return (
        "planned_start_time" in operation_class.model_fields
        and "planned_finish_time" in operation_class.model_fields
    )


def _get_datetime_field(operation: Operation, field_name: str):
    if hasattr(operation, field_name):
        return getattr(operation, field_name)
    raise TypeError(
        f"operation of type {type(operation).__name__} does not define {field_name}",
    )
