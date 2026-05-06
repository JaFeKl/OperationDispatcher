from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class RuntimeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"


class ResultStatus(str, Enum):
    NONE = "none"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STOPPED = "stopped"


class Operation(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    agent_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    runtime_status: RuntimeStatus = RuntimeStatus.PENDING
    result_status: ResultStatus = ResultStatus.NONE
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
