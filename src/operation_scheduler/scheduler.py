from __future__ import annotations

import asyncio
import inspect
from typing import cast
from datetime import datetime, timezone
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from .models import Operation, ResultStatus, RuntimeStatus
from .schedule import Schedule


class Scheduler:
    def __init__(
        self,
        schedule: Schedule | None = None,
        operation_executor: Callable[[Operation], object] | None = None,
        poll_interval_seconds: float = 0.1,
        payload_model: Any | None = None,
    ) -> None:
        self._schedule = schedule if schedule is not None else Schedule()
        self._operation_executor = operation_executor
        self._payload_model = payload_model
        self._current_operation: Operation | None = None
        self._is_paused = False
        self._is_running = False
        self._stop_requested = False
        self._poll_interval_seconds = poll_interval_seconds
        self._running_since: datetime | None = None

    @property
    def schedule(self) -> Schedule:
        return self._schedule

    @property
    def current_operation(self) -> Operation | None:
        return self._current_operation

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def payload_model(self) -> Any | None:
        return self._payload_model

    def add(self, operation: Operation) -> None:
        self._schedule.add(operation)

    def get_schedule(self) -> list[Operation]:
        return self._schedule.list()

    def get_state(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        running_since = self._running_since
        uptime_seconds: float | None = None
        if self._is_running and running_since is not None:
            uptime_seconds = (now - running_since).total_seconds()

        return {
            "is_running": self._is_running,
            "is_paused": self._is_paused,
            "queue_size": len(self._schedule),
            "current_operation": (
                self._current_operation.model_dump(mode="json")
                if self._current_operation is not None
                else None
            ),
            "running_since": (
                running_since.isoformat() if running_since is not None else None
            ),
            "uptime_seconds": uptime_seconds,
        }

    def pause(self) -> None:
        self._is_paused = True

    def resume(self) -> None:
        self._is_paused = False

    def start_next(self) -> Operation | None:
        if self._is_paused:
            return None

        if self._current_operation is not None:
            raise RuntimeError("an operation is already running")

        operation = self._schedule.next()
        if operation is None:
            return None

        self._current_operation = operation
        return operation

    async def run_once(self) -> Operation | None:
        if self._is_paused or self._current_operation is not None:
            return None

        next_operation = self._schedule.peek()
        if next_operation is None:
            return None

        if next_operation.time_window is not None:
            now = datetime.now(timezone.utc)
            if next_operation.time_window.start > now:
                return None

        operation = self.start_next()
        if operation is None:
            return None

        try:
            await self._execute_operation(operation)
            self.complete_current()
        except Exception:
            self.fail_current()
            raise

        return operation

    async def run(self) -> None:
        if self._is_running:
            raise RuntimeError("scheduler is already running")

        self._is_running = True
        self._running_since = datetime.now(timezone.utc)
        self._stop_requested = False
        try:
            while not self._stop_requested:
                try:
                    await self.run_once()
                except Exception:
                    pass
                await asyncio.sleep(self._poll_interval_seconds)
        finally:
            self._is_running = False
            self._running_since = None

    def request_stop(self) -> None:
        self._stop_requested = True

    def complete_current(self) -> Operation:
        operation = self._require_current_operation()

        if operation.runtime_status is RuntimeStatus.RUNNING:
            self._schedule.complete(operation)
        elif operation.runtime_status is RuntimeStatus.FINISHED:
            self._set_timed_operation_finish_if_needed(operation)

        self._current_operation = None
        return operation

    def fail_current(self) -> Operation:
        operation = self._require_current_operation()
        operation.runtime_status = RuntimeStatus.FINISHED
        operation.result_status = ResultStatus.FAILED
        self._set_timed_operation_finish_if_needed(operation)
        self._current_operation = None
        return operation

    def stop_current(self) -> Operation:
        operation = self._require_current_operation()
        operation.runtime_status = RuntimeStatus.FINISHED
        operation.result_status = ResultStatus.STOPPED
        self._set_timed_operation_finish_if_needed(operation)
        self._current_operation = None
        return operation

    def cancel(self, operation_id: UUID) -> Operation | None:
        if (
            self._current_operation is not None
            and self._current_operation.id == operation_id
        ):
            return self.stop_current()
        return self._schedule.cancel(operation_id)

    def _require_current_operation(self) -> Operation:
        if self._current_operation is None:
            raise RuntimeError("no current operation")
        return self._current_operation

    async def _execute_operation(self, operation: Operation) -> None:
        if self._operation_executor is None:
            return

        execution_result = self._operation_executor(operation)
        if inspect.isawaitable(execution_result):
            await _as_awaitable(execution_result)

    @staticmethod
    def _set_timed_operation_finish_if_needed(operation: Operation) -> None:
        if operation.finish_time is None:
            operation.finish_time = datetime.now(timezone.utc)


def _as_awaitable(result: object) -> Awaitable[object]:
    return cast(Awaitable[object], result)
