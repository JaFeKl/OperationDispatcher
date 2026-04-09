from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class OperationStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class OperationPayload(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)


class Operation(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    agent_id: str
    payload: OperationPayload = Field(default_factory=OperationPayload)
    priority: int = 0
    status: OperationStatus = OperationStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TimedOperation(Operation):
    planned_start_time: datetime
    planned_finish_time: datetime
    actual_start_time: datetime | None = None
    actual_finish_time: datetime | None = None

    @model_validator(mode="after")
    def validate_timestamps(self) -> "TimedOperation":
        if self.planned_finish_time < self.planned_start_time:
            raise ValueError("planned_finish_time must be after planned_start_time")

        if (
            self.actual_start_time is not None
            and self.actual_finish_time is not None
            and self.actual_finish_time < self.actual_start_time
        ):
            raise ValueError("actual_finish_time must be after actual_start_time")

        return self
