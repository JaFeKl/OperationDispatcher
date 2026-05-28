from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, model_validator


def _normalize_to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class ExecutionState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ExecutionOutcome(str, Enum):
    NONE = "NONE"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"


class TerminationReason(str, Enum):
    NONE = "NONE"
    USER_REQUEST = "USER_REQUEST"
    TIMEOUT = "TIMEOUT"
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    EXTERNAL_ERROR = "EXTERNAL_ERROR"


class EventType(str, Enum):
    OPERATION_DISPATCHER_STARTED = "operation_dispatcher_started"
    OPERATION_DISPATCHER_STOPPED = "operation_dispatcher_stopped"
    OPERATION_DISPATCHER_PAUSED = "operation_dispatcher_paused"
    OPERATION_DISPATCHER_RESUMED = "operation_dispatcher_resumed"

    OPERATION_START_REQUESTED = "operation_start_requested"
    OPERATION_START_DENIED = "operation_start_denied"
    OPERATION_CANCEL_REQUESTED = "operation_cancel_requested"
    OPERATION_CANCEL_DENIED = "operation_cancel_denied"
    OPERATION_PAUSE_REQUESTED = "operation_pause_requested"
    OPERATION_PAUSE_DENIED = "operation_pause_denied"
    OPERATION_RESUME_REQUESTED = "operation_resume_requested"
    OPERATION_RESUME_DENIED = "operation_resume_denied"

    OPERATION_ADDED = "operation_added"
    OPERATION_UPDATED = "operation_updated"
    OPERATION_STARTED = "operation_started"
    OPERATION_COMPLETED = "operation_completed"
    OPERATION_FAILED = "operation_failed"
    OPERATION_PAUSED = "operation_paused"
    OPERATION_RESUMED = "operation_resumed"
    OPERATION_CANCELLED = "operation_cancelled"


class DependencyType(str, Enum):
    FINISH_TO_START = "FINISH_TO_START"
    START_TO_START = "START_TO_START"


class ChangeRecord(BaseModel):
    field: str
    old_value: Any
    new_value: Any


class DispatchEvent(BaseModel):
    """
    A record of an event that occurred during the lifecycle of an operation,
    such as start, completion, failure, etc.
    """

    id: UUID = Field(default_factory=uuid4)
    operation_id: UUID | None = None
    event_type: EventType
    changes: list[ChangeRecord] = Field(default_factory=list)
    meta_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def normalize_created_at(self) -> "DispatchEvent":
        self.created_at = _normalize_to_utc(self.created_at)  # type: ignore[assignment]
        return self


class Operation(BaseModel):
    """
    An operation scheduled for execution on an agent/resource.
    The payload field is user-defined and can contain any information needed
    to execute the operation, such as command, parameters, etc.
    """

    id: UUID = Field(default_factory=uuid4)

    # user payload describing the operation to be performed, e.g. command, parameters, etc.
    payload: dict[str, Any] = Field(default_factory=dict)

    # Scheduling
    resource_id: str
    priority: int = 0
    release_date: datetime | None = None
    planned_duration: int | None = None
    due_date: datetime | None = None
    dependencies: list[UUID] = Field(default_factory=list)

    # Runtime state
    state: ExecutionState = ExecutionState.QUEUED
    outcome: ExecutionOutcome = ExecutionOutcome.NONE
    termination_reason: TerminationReason = TerminationReason.NONE
    retry_count: int = 0
    start_time: datetime | None = None
    finish_time: datetime | None = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_dates(self) -> "Operation":
        self.created_at = _normalize_to_utc(self.created_at)  # type: ignore[assignment]
        self.release_date = _normalize_to_utc(self.release_date)  # type: ignore[assignment]
        self.due_date = _normalize_to_utc(self.due_date)  # type: ignore[assignment]
        self.start_time = _normalize_to_utc(self.start_time)  # type: ignore[assignment]
        self.finish_time = _normalize_to_utc(self.finish_time)  # type: ignore[assignment]

        if self.planned_duration is not None and self.planned_duration <= 0:
            raise ValueError("planned_duration must be > 0")
        if (
            self.release_date is not None
            and self.due_date is not None
            and self.due_date <= self.release_date
        ):
            raise ValueError("due_date must be after release_date")
        if (
            self.start_time is not None
            and self.finish_time is not None
            and self.finish_time < self.start_time
        ):
            raise ValueError("finish time must be after start time")
        if self.state == ExecutionState.RUNNING and self.start_time is None:
            raise ValueError("running operation requires actual_start_time")
        return self


class OperationDependency(BaseModel):
    """
    A record of a dependency between two operations.
    For example, operation A depends on operation B to finish before it can start.
    This is used to enforce execution order constraints between operations.
    """

    id: UUID = Field(default_factory=uuid4)
    operation_id: UUID
    depends_on_operation_id: UUID
    dependency_type: DependencyType
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_unique_dependency(self) -> OperationDependency:
        self.created_at = _normalize_to_utc(self.created_at)  # type: ignore[assignment]
        if self.operation_id == self.depends_on_operation_id:
            raise ValueError("operation cannot depend on itself")
        return self


class OperationDispatcherState(BaseModel):
    """
    A record of the current state of the operation dispatcher, including whether it is running or paused, the size of the queue, and details about the currently running operation if applicable.
    """

    is_running: bool
    is_paused: bool
    queue_size: int
    current_operation: Operation | None = None
    running_since: datetime | None = None
    uptime_seconds: float | None = None


class HistoryRecord(BaseModel):
    """
    A record of a completed operation, including its final state and outcome, as well as any events that occurred during its execution.
    """

    operation: Operation
    events: list[DispatchEvent] = Field(default_factory=list)


class History(BaseModel):
    """
    A record of completed operations, including their final state and outcome, as well as any events that occurred during their execution.
    """

    num_records: int = 0
    records: list[HistoryRecord] = Field(default_factory=list)
