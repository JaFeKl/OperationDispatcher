from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

from .models import ScheduledOperation


class SortField(str, Enum):
    PRIORITY = "priority"
    RELEASE_DATE = "release_date"
    CREATED_AT = "created_at"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True)
class SortRule:
    field: SortField
    direction: SortDirection = SortDirection.ASC
    none_last: bool = True


class DispatchQueue:
    """Priority dispatch queue for one agent with at most one active operation.

    The dispatch queue keeps operations in three lifecycle buckets:
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
        resource_id: str,
        operations: Iterable[ScheduledOperation] | None = None,
        sort_rules: Iterable[SortRule] | None = None,
    ) -> None:
        self._resource_id = resource_id
        self._sort_rules = self._resolve_sort_rules(sort_rules)
        self._queue: list[ScheduledOperation] = []
        self._pulled: ScheduledOperation | None = None
        self._completed: list[ScheduledOperation] = []

        for operation in operations or []:
            self.add(operation)

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def resource_id(self) -> str:
        return self._resource_id

    @property
    def pulled_operation(self) -> ScheduledOperation | None:
        return self._pulled

    @property
    def completed_operations(self) -> list[ScheduledOperation]:
        return list(self._completed)

    @property
    def sort_rules(self) -> tuple[SortRule, ...]:
        return tuple(self._sort_rules)

    def history(self, limit: int | None = None) -> list[ScheduledOperation]:
        operations = list(reversed(self._completed))
        if limit is None:
            return operations
        return operations[:limit]

    def add(self, operation: ScheduledOperation) -> None:
        self._validate_operation(operation)
        self._queue.append(operation)
        self._sort_operations()

    def get(self, operation_id: UUID) -> ScheduledOperation | None:
        """
        Get an operation by ID from the schedule, including pending, pulled and completed operations.
        """
        if self._pulled is not None:
            if self._pulled.id == operation_id:
                return self._pulled
        for scheduled_operation in self._queue:
            if scheduled_operation.id == operation_id:
                return scheduled_operation
        for scheduled_operation in self._completed:
            if scheduled_operation.id == operation_id:
                return scheduled_operation
        return None

    def next(self) -> ScheduledOperation | None:
        if self._pulled is not None:
            raise RuntimeError(
                "cannot pull next operation while another operation is active"
            )
        if not self._queue:
            return None

        operation = self._queue.pop(0)
        self._pulled = operation
        return operation

    def peek(self) -> ScheduledOperation | None:
        if not self._queue:
            return None
        return self._queue[0]

    def complete(self, operation: ScheduledOperation) -> None:
        self._validate_operation(operation)

        if self._pulled is None or self._pulled is not operation:
            raise ValueError(
                "operation must be pulled from this schedule before completion"
            )
        self._archive_completed_operation(operation)
        self._pulled = None

    def list(self) -> list[ScheduledOperation]:
        return list(self._queue)

    def clear(self) -> None:
        self._queue.clear()

    def clear_history(self) -> None:
        self._completed.clear()

    def remove(self, operation_id: UUID) -> ScheduledOperation | None:
        for index, operation in enumerate(self._queue):
            if operation.id == operation_id:
                return self._queue.pop(index)
        return None

    def cancel(self, operation_id: UUID) -> ScheduledOperation | None:
        if self._pulled is not None and self._pulled.id == operation_id:
            operation = self._pulled
            self._archive_completed_operation(operation)
            self._pulled = None
            return operation

        operation = self.remove(operation_id)
        if operation is None:
            return None
        self._archive_completed_operation(operation)
        return operation

    def _archive_completed_operation(self, operation: ScheduledOperation) -> None:
        if operation not in self._completed:
            self._completed.append(operation)

    def _sort_operations(self) -> None:
        self._queue.sort(key=self._operation_sort_key)

    def _operation_sort_key(
        self, operation: ScheduledOperation
    ) -> tuple[tuple[int, float], ...]:
        return tuple(
            self._rule_key_component(operation, rule) for rule in self._sort_rules
        )

    def _rule_key_component(
        self,
        operation: ScheduledOperation,
        rule: SortRule,
    ) -> tuple[int, float]:
        raw_value = self._field_value(operation, rule.field)

        if raw_value is None:
            none_rank = 1 if rule.none_last else 0
            return none_rank, 0.0

        none_rank = 0 if rule.none_last else 1

        normalized_value: float
        if isinstance(raw_value, datetime):
            normalized_value = raw_value.timestamp()
        else:
            normalized_value = float(raw_value)

        if rule.direction is SortDirection.DESC:
            normalized_value = -normalized_value

        return none_rank, normalized_value

    @staticmethod
    def _field_value(
        operation: ScheduledOperation,
        field: SortField,
    ) -> int | datetime | None:
        if field is SortField.PRIORITY:
            return operation.priority
        if field is SortField.RELEASE_DATE:
            return operation.release_date
        if field is SortField.CREATED_AT:
            return operation.created_at
        raise ValueError(f"unsupported sort field: {field}")

    @staticmethod
    def _resolve_sort_rules(
        sort_rules: Iterable[SortRule] | None,
    ) -> list[SortRule]:
        if sort_rules is None:
            resolved_rules = [
                SortRule(
                    field=SortField.RELEASE_DATE,
                    direction=SortDirection.ASC,
                    none_last=True,
                ),
                SortRule(
                    field=SortField.PRIORITY,
                    direction=SortDirection.DESC,
                    none_last=True,
                ),
            ]
        else:
            resolved_rules = list(sort_rules)

        if not resolved_rules:
            raise ValueError("sort_rules must contain at least one rule")

        if all(rule.field is not SortField.CREATED_AT for rule in resolved_rules):
            resolved_rules.append(
                SortRule(
                    field=SortField.CREATED_AT,
                    direction=SortDirection.ASC,
                    none_last=True,
                )
            )

        return resolved_rules

    def _validate_operation(self, operation: ScheduledOperation) -> None:
        if self._resource_id is not None and operation.resource_id != self._resource_id:
            raise ValueError(
                "operation resource_id does not match schedule resource_id",
            )
