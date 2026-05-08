from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class LifecycleStatus(str, Enum):
    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    FINISHED = "finished"


class ExecutionOutcome(str, Enum):
    NONE = "none"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TerminationReason(str, Enum):
    NONE = "none"
    CANCELLED_BEFORE_START = "cancelled_before_start"
    CANCELLED_DURING_RUN = "cancelled_during_run"
    STOPPED = "stopped"
    TIMEOUT = "timeout"
    DEPENDENCY_FAILED = "dependency_failed"


class OperationManagerEventType(str, Enum):
    OPERATION_MANAGER_STARTED = "operation_manager_started"
    OPERATION_MANAGER_STOPPED = "operation_manager_stopped"
    OPERATION_MANAGER_PAUSED = "operation_manager_paused"
    OPERATION_MANAGER_RESUMED = "operation_manager_resumed"

    OPERATION_START_REQUESTED = "operation_start_requested"
    OPERATION_START_DENIED = "operation_start_denied"
    OPERATION_START_DISPATCH_REQUESTED = "operation_start_dispatch_requested"
    OPERATION_START_DISPATCH_DENIED = "operation_start_dispatch_denied"
    OPERATION_CANCEL_REQUESTED = "operation_cancel_requested"
    OPERATION_CANCEL_DENIED = "operation_cancel_denied"
    OPERATION_STOP_REQUESTED = "operation_stop_requested"
    OPERATION_STOP_DENIED = "operation_stop_denied"
    OPERATION_RESUME_REQUESTED = "operation_resume_requested"
    OPERATION_RESUME_DENIED = "operation_resume_denied"

    OPERATION_ADDED = "operation_added"
    OPERATION_STARTED = "operation_started"
    OPERATION_COMPLETED = "operation_completed"
    OPERATION_FAILED = "operation_failed"
    OPERATION_STOPPED = "operation_stopped"
    OPERATION_CANCELLED = "operation_cancelled"


class OperationManagerEvent(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_type: OperationManagerEventType
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    agent_id: str | None = None
    operation_id: UUID | None = None
    operation_name: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class Operation(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    agent_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    lifecycle_status: LifecycleStatus = LifecycleStatus.QUEUED
    execution_outcome: ExecutionOutcome = ExecutionOutcome.NONE
    termination_reason: TerminationReason = TerminationReason.NONE
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # actual execution times, set when operation is started/finished
    start_time: datetime | None = None
    finish_time: datetime | None = None

    # scheduling planning times
    time_window: TimeWindow | None = None


class TimeWindow(BaseModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_window(self) -> "TimeWindow":
        if self.end <= self.start:
            raise ValueError("end must be after start")
        return self


class OperationManagerState(BaseModel):
    is_running: bool
    is_paused: bool
    queue_size: int
    current_operation: Operation | None = None
    running_since: datetime | None = None
    uptime_seconds: float | None = None
