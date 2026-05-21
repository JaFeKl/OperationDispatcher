from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, model_validator


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
    OPERATION_MANAGER_STARTED = "operation_manager_started"
    OPERATION_MANAGER_STOPPED = "operation_manager_stopped"
    OPERATION_MANAGER_PAUSED = "operation_manager_paused"
    OPERATION_MANAGER_RESUMED = "operation_manager_resumed"

    OPERATION_START_REQUESTED = "operation_start_requested"
    OPERATION_START_DENIED = "operation_start_denied"
    OPERATION_CANCEL_REQUESTED = "operation_cancel_requested"
    OPERATION_CANCEL_DENIED = "operation_cancel_denied"
    OPERATION_PAUSE_REQUESTED = "operation_pause_requested"
    OPERATION_PAUSE_DENIED = "operation_pause_denied"
    OPERATION_RESUME_REQUESTED = "operation_resume_requested"
    OPERATION_RESUME_DENIED = "operation_resume_denied"

    OPERATION_ADDED = "operation_added"
    OPERATION_STARTED = "operation_started"
    OPERATION_COMPLETED = "operation_completed"
    OPERATION_FAILED = "operation_failed"
    OPERATION_PAUSED = "operation_paused"
    OPERATION_CANCELLED = "operation_cancelled"


class DependencyType(str, Enum):
    FINISH_TO_START = "FINISH_TO_START"
    START_TO_START = "START_TO_START"


class DispatchEvent(BaseModel):
    """
    A record of an event that occurred during the lifecycle of an operation,
    such as start, completion, failure, etc.
    """

    id: UUID = Field(default_factory=uuid4)
    execution_id: UUID
    operation_id: UUID
    event_type: EventType
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default_factory=dict)


class ScheduledOperation(BaseModel):
    """
    An operation scheduled for execution on an agent/resource.
    The payload field is user-defined and can contain any information needed
    to execute the operation, such as command, parameters, etc.
    """

    id: UUID = Field(default_factory=uuid4)
    payload: dict[str, Any] = Field(default_factory=dict)
    resource_id: str
    priority: int = 0
    release_date: datetime | None = None
    planned_duration: timedelta | None = None
    due_date: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_scheduled_operation(self) -> "ScheduledOperation":
        if self.planned_duration is not None and self.planned_duration <= timedelta(0):
            raise ValueError("planned_duration must be > 0")
        if (
            self.release_date is not None
            and self.due_date is not None
            and self.due_date <= self.release_date
        ):
            raise ValueError("due_date must be after release_date")
        return self


class OperationExecution(BaseModel):
    """
    A record of the execution of an operation, including its current state, outcome, retry count, and timestamps.
    """

    id: UUID = Field(default_factory=uuid4)
    operation_id: UUID
    state: ExecutionState = ExecutionState.QUEUED
    outcome: ExecutionOutcome = ExecutionOutcome.NONE
    termination_reason: TerminationReason = TerminationReason.NONE
    retry_count: int = 0
    start_time: datetime | None = None
    finish_time: datetime | None = None

    @model_validator(mode="after")
    def validate_execution(self) -> "OperationExecution":
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
        if self.operation_id == self.depends_on_operation_id:
            raise ValueError("operation cannot depend on itself")
        return self


class OperationHistoryEntry(BaseModel):
    scheduled_operation: ScheduledOperation
    execution: OperationExecution
    events: list[DispatchEvent] = Field(default_factory=list)


class OperationDispatcherState(BaseModel):
    is_running: bool
    is_paused: bool
    queue_size: int
    current_operation: ScheduledOperation | None = None
    running_since: datetime | None = None
    uptime_seconds: float | None = None
