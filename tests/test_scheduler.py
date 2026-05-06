import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from operation_scheduler import (
    Operation,
    ResultStatus,
    RuntimeStatus,
    Schedule,
    Scheduler,
    TimeWindow,
)


def test_scheduler_starts_and_completes_next_operation() -> None:
    schedule = Schedule()
    scheduler = Scheduler(schedule=schedule)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    current = scheduler.start_next()

    assert current is operation
    assert scheduler.current_operation is operation
    assert operation.runtime_status is RuntimeStatus.RUNNING
    assert operation.result_status is ResultStatus.NONE

    completed = scheduler.complete_current()
    assert completed is operation
    assert scheduler.current_operation is None
    assert schedule.completed_operations == [operation]


def test_scheduler_run_once_executes_and_completes_operation() -> None:
    scheduler = Scheduler()
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert scheduler.current_operation is None
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.SUCCEEDED
    assert scheduler.schedule.completed_operations == [operation]


def test_scheduler_pause_blocks_start_until_resumed() -> None:
    scheduler = Scheduler()
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    scheduler.pause()
    assert scheduler.start_next() is None

    scheduler.resume()
    assert scheduler.start_next() is operation


def test_scheduler_cancels_pending_operation() -> None:
    scheduler = Scheduler()
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is operation
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.CANCELLED
    assert scheduler.schedule.next() is None


def test_scheduler_cancel_stops_current_operation() -> None:
    scheduler = Scheduler()
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler.start_next()

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is operation
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.STOPPED
    assert scheduler.current_operation is None


def test_scheduler_fail_current_sets_finish_time() -> None:
    now = datetime.now(timezone.utc)
    operation = Operation(
        name="timed",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now,
            end=now + timedelta(minutes=10),
        ),
    )
    scheduler = Scheduler(schedule=Schedule())
    scheduler.add(operation)
    scheduler.start_next()

    failed = scheduler.fail_current()

    assert failed is operation
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.FAILED
    assert operation.finish_time is not None


def test_scheduler_marks_failed_if_executor_raises() -> None:
    def broken_executor(operation: Operation) -> None:
        raise RuntimeError("boom")

    scheduler = Scheduler(operation_executor=broken_executor)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    with pytest.raises(RuntimeError):
        asyncio.run(scheduler.run_once())

    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.FAILED
    assert scheduler.current_operation is None


def test_scheduler_run_once_waits_for_windowed_operation_due_time() -> None:
    now = datetime.now(timezone.utc)
    operation = Operation(
        name="timed",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=2),
            end=now + timedelta(minutes=10),
        ),
    )
    scheduler = Scheduler(schedule=Schedule())
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is None
    assert operation.runtime_status is RuntimeStatus.PENDING
    assert operation.result_status is ResultStatus.NONE
    assert scheduler.current_operation is None


def test_scheduler_run_once_supports_async_executor() -> None:
    called_with: list[Operation] = []

    async def async_executor(operation: Operation) -> None:
        await asyncio.sleep(0)
        called_with.append(operation)

    scheduler = Scheduler(operation_executor=async_executor)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert called_with == [operation]
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.SUCCEEDED


def test_scheduler_preserves_executor_set_result_status() -> None:
    def status_setting_executor(operation: Operation) -> None:
        operation.runtime_status = RuntimeStatus.FINISHED
        operation.result_status = ResultStatus.FAILED

    scheduler = Scheduler(operation_executor=status_setting_executor)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.FAILED
    assert scheduler.current_operation is None
    assert scheduler.schedule.completed_operations == []


def test_scheduler_run_loop_processes_operations_until_stopped() -> None:
    scheduler = Scheduler(poll_interval_seconds=0.01)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    async def run_scheduler() -> None:
        task = asyncio.create_task(scheduler.run())
        await asyncio.sleep(0.05)
        scheduler.request_stop()
        await task

    asyncio.run(run_scheduler())

    assert operation.runtime_status is RuntimeStatus.FINISHED
    assert operation.result_status is ResultStatus.SUCCEEDED
    assert scheduler.current_operation is None


def test_scheduler_get_state_reports_runtime_and_queue_information() -> None:
    scheduler = Scheduler()
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    initial_state = scheduler.get_state()
    assert initial_state["is_running"] is False
    assert initial_state["queue_size"] == 1
    assert initial_state["current_operation"] is None
    assert initial_state["running_since"] is None
    assert initial_state["uptime_seconds"] is None

    scheduler.start_next()
    running_state = scheduler.get_state()
    assert running_state["current_operation"] is not None
    assert running_state["current_operation"]["id"] == str(operation.id)
