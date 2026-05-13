import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from operation_dispatcher import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    OperationManager,
    OperationManagerEventType,
    OperationManagerState,
    Schedule,
    TerminationReason,
    TimeWindow,
)

Scheduler = OperationManager
SchedulerEventType = OperationManagerEventType
SchedulerState = OperationManagerState


def test_scheduler_starts_and_completes_next_operation() -> None:
    scheduler = Scheduler(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    current = scheduler._start_next()

    assert current is operation
    assert scheduler.current_operation is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE

    completed = scheduler.complete_current()
    assert completed is operation
    assert scheduler.current_operation is None
    assert scheduler.schedule.completed_operations == [operation]


def test_scheduler_run_once_executes_and_completes_operation() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        return True

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert seen_event_types == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_STARTED,
    ]
    assert scheduler.current_operation is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE
    assert scheduler.schedule.completed_operations == []


def test_scheduler_pause_blocks_start_until_resumed() -> None:
    scheduler = Scheduler(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    scheduler.pause()
    assert scheduler._start_next() is None

    scheduler.resume()
    assert scheduler._start_next() is operation


def test_scheduler_cancels_pending_operation() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_CANCEL_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is operation
    assert operation.lifecycle_status is LifecycleStatus.FINISHED
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.CANCELLED_BEFORE_START
    assert scheduler.schedule.next() is None
    assert seen_events == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_CANCEL_REQUESTED,
        SchedulerEventType.OPERATION_CANCELLED,
    ]


def test_scheduler_cancel_marks_current_operation_as_cancelled_during_run() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_CANCEL_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is operation
    assert operation.lifecycle_status is LifecycleStatus.FINISHED
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.CANCELLED_DURING_RUN
    assert scheduler.current_operation is None
    assert seen_events == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_STARTED,
        SchedulerEventType.OPERATION_CANCEL_REQUESTED,
        SchedulerEventType.OPERATION_CANCELLED,
    ]


def test_scheduler_cancelled_running_operation_is_archived_in_history() -> None:
    scheduler = Scheduler(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    scheduler.cancel(operation.id)

    assert scheduler.schedule.completed_operations == [operation]
    assert scheduler.schedule.history(limit=1) == [operation]


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
    scheduler = Scheduler(agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    failed = scheduler.fail_current()

    assert failed is operation
    assert operation.lifecycle_status is LifecycleStatus.FINISHED
    assert operation.execution_outcome is ExecutionOutcome.FAILED
    assert operation.termination_reason is TerminationReason.NONE
    assert operation.finish_time is not None


def test_scheduler_marks_failed_if_executor_raises() -> None:
    def deny_start(event) -> bool | None:
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=deny_start,
        on_notification_callback=deny_start,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE
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
    scheduler = Scheduler(agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE
    assert scheduler.current_operation is None


def test_scheduler_run_once_supports_async_executor() -> None:
    async def request_and_notification_callback(event) -> bool | None:
        await asyncio.sleep(0)
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE


def test_scheduler_preserves_executor_set_result_status() -> None:
    seen_start_request = False

    def request_and_notification_callback(event) -> bool | None:
        nonlocal seen_start_request
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            seen_start_request = True
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE
    assert seen_start_request is True
    assert scheduler.current_operation is operation
    assert scheduler.schedule.completed_operations == []


def test_scheduler_run_loop_processes_operations_until_stopped() -> None:
    scheduler = Scheduler(
        agent_id="agent-a",
        poll_interval_seconds=0.01,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    async def run_scheduler() -> None:
        task = asyncio.create_task(scheduler.run())
        await asyncio.sleep(0.05)
        scheduler.request_stop()
        await task

    asyncio.run(run_scheduler())

    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE
    assert scheduler.current_operation is operation


def test_scheduler_get_state_reports_runtime_and_queue_information() -> None:
    scheduler = Scheduler(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    initial_state = scheduler.get_state()
    assert isinstance(initial_state, SchedulerState)
    assert initial_state.is_running is False
    assert initial_state.queue_size == 1
    assert initial_state.current_operation is None
    assert initial_state.running_since is None
    assert initial_state.uptime_seconds is None

    scheduler._start_next()
    running_state = scheduler.get_state()
    assert running_state.current_operation is not None
    assert running_state.current_operation.id == operation.id


def test_scheduler_emits_events_for_operation_lifecycle() -> None:
    seen_events: list[SchedulerEventType] = []

    def notification_callback(event) -> None:
        seen_events.append(event.event_type)

    scheduler = Scheduler(
        agent_id="agent-a",
        on_notification_callback=notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")

    scheduler.add(operation)
    scheduler._start_next()
    scheduler.complete_current()

    assert seen_events == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_STARTED,
        SchedulerEventType.OPERATION_COMPLETED,
    ]


def test_scheduler_run_loop_wakes_up_on_added_operation() -> None:
    scheduler = Scheduler(
        agent_id="agent-a",
        poll_interval_seconds=60.0,
    )
    operation = Operation(name="sync", agent_id="agent-a")

    async def run_scheduler() -> None:
        task = asyncio.create_task(scheduler.run())
        await asyncio.sleep(0.05)
        scheduler.add(operation)
        await asyncio.sleep(0.15)
        scheduler.request_stop()
        await task

    asyncio.run(run_scheduler())

    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE


def test_scheduler_records_scheduler_lifecycle_events() -> None:
    scheduler = Scheduler(
        agent_id="agent-a",
        poll_interval_seconds=0.01,
    )

    async def run_scheduler() -> None:
        task = asyncio.create_task(scheduler.run())
        await asyncio.sleep(0.05)
        scheduler.request_stop()
        await task

    asyncio.run(run_scheduler())
    event_types = [event.event_type for event in scheduler.get_event_history()]

    assert SchedulerEventType.OPERATION_MANAGER_STARTED in event_types
    assert SchedulerEventType.OPERATION_MANAGER_STOPPED in event_types


def test_scheduler_stop_current_emits_stop_requested_before_stopped() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_STOP_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    scheduler.stop_current()

    assert seen_events == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_STARTED,
        SchedulerEventType.OPERATION_STOP_REQUESTED,
        SchedulerEventType.OPERATION_STOPPED,
    ]


def test_scheduler_resume_emits_operation_resume_requested_when_current_exists() -> (
    None
):
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_RESUME_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    scheduler.pause()
    scheduler.resume()

    assert seen_events == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_STARTED,
        SchedulerEventType.OPERATION_MANAGER_PAUSED,
        SchedulerEventType.OPERATION_RESUME_REQUESTED,
        SchedulerEventType.OPERATION_MANAGER_RESUMED,
    ]


def test_scheduler_calls_notification_callback_for_emitted_events() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def notification_callback(event) -> None:
        seen_event_types.append(event.event_type)

    scheduler = Scheduler(
        agent_id="agent-a",
        on_notification_callback=notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")

    scheduler.add(operation)
    scheduler._start_next()
    scheduler.complete_current()

    assert seen_event_types == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_STARTED,
        SchedulerEventType.OPERATION_COMPLETED,
    ]


def test_scheduler_run_once_emits_start_requested_event() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert SchedulerEventType.OPERATION_START_REQUESTED in seen_event_types


def test_scheduler_does_not_continue_when_start_request_callback_returns_none() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert scheduler.current_operation is None
    assert SchedulerEventType.OPERATION_START_REQUESTED in seen_event_types
    assert SchedulerEventType.OPERATION_START_DENIED in seen_event_types


def test_scheduler_retries_denied_start_after_cooldown() -> None:
    seen_event_types: list[SchedulerEventType] = []
    start_request_calls = 0

    def request_and_notification_callback(event) -> bool | None:
        nonlocal start_request_calls
        seen_event_types.append(event.event_type)

        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            start_request_calls += 1
            return start_request_calls >= 2

        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
        start_request_retry_cooldown_seconds=0.02,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    async def run_attempts() -> Operation | None:
        first = await scheduler.run_once()
        second = await scheduler.run_once()
        await asyncio.sleep(0.03)
        third = await scheduler.run_once()

        assert first is None
        assert second is None
        return third

    executed = asyncio.run(run_attempts())

    assert executed is operation
    assert start_request_calls == 2
    assert seen_event_types == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_START_DENIED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_STARTED,
    ]


def test_scheduler_pauses_when_denied_start_reaches_max_retries() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
        start_request_max_retries=2,
        start_request_retry_cooldown_seconds=0.0,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    first = asyncio.run(scheduler.run_once())
    second = asyncio.run(scheduler.run_once())

    assert first is None
    assert second is None
    assert scheduler.is_paused is True
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert scheduler.current_operation is None
    assert seen_event_types == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_START_DENIED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_START_DENIED,
        SchedulerEventType.OPERATION_MANAGER_PAUSED,
    ]


def test_scheduler_resume_resets_denied_start_retry_counter() -> None:
    start_request_calls = 0

    def request_and_notification_callback(event) -> bool | None:
        nonlocal start_request_calls
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            start_request_calls += 1
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
        start_request_max_retries=2,
        start_request_retry_cooldown_seconds=0.0,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    asyncio.run(scheduler.run_once())
    asyncio.run(scheduler.run_once())
    assert scheduler.is_paused is True

    scheduler.resume()
    assert scheduler.is_paused is False

    third_attempt = asyncio.run(scheduler.run_once())

    assert third_attempt is None
    assert scheduler.is_paused is False
    assert start_request_calls == 3

    fourth_attempt = asyncio.run(scheduler.run_once())

    assert fourth_attempt is None
    assert scheduler.is_paused is True
    assert start_request_calls == 4


def test_scheduler_starts_when_start_request_is_allowed() -> None:
    seen_event_types: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert scheduler.current_operation is operation
    assert seen_event_types == [
        SchedulerEventType.OPERATION_ADDED,
        SchedulerEventType.OPERATION_START_REQUESTED,
        SchedulerEventType.OPERATION_STARTED,
    ]


def test_scheduler_request_event_callback_timeout_denies_start() -> None:
    seen_event_types: list[SchedulerEventType] = []

    async def request_and_notification_callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            await asyncio.sleep(0.05)
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
        request_event_timeout_seconds=0.01,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    executed = asyncio.run(scheduler.run_once())

    assert executed is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert scheduler.current_operation is None
    assert SchedulerEventType.OPERATION_START_REQUESTED in seen_event_types
    assert SchedulerEventType.OPERATION_START_DENIED in seen_event_types


def test_scheduler_does_not_cancel_when_cancel_request_is_denied() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_CANCEL_REQUESTED:
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
    assert scheduler.current_operation is None
    assert SchedulerEventType.OPERATION_CANCEL_REQUESTED in seen_events
    assert SchedulerEventType.OPERATION_CANCEL_DENIED in seen_events
    assert SchedulerEventType.OPERATION_CANCELLED not in seen_events


def test_scheduler_does_not_stop_when_stop_request_is_denied() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_STOP_REQUESTED:
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()

    stopped = scheduler.stop_current()

    assert stopped is operation
    assert scheduler.current_operation is operation
    assert operation.lifecycle_status is LifecycleStatus.RUNNING
    assert SchedulerEventType.OPERATION_STOP_REQUESTED in seen_events
    assert SchedulerEventType.OPERATION_STOP_DENIED in seen_events
    assert SchedulerEventType.OPERATION_STOPPED not in seen_events


def test_scheduler_does_not_resume_when_resume_request_is_denied() -> None:
    seen_events: list[SchedulerEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is SchedulerEventType.OPERATION_RESUME_REQUESTED:
            return False
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)
    scheduler._start_next()
    scheduler.pause()

    scheduler.resume()

    assert scheduler.is_paused is True
    assert SchedulerEventType.OPERATION_RESUME_REQUESTED in seen_events
    assert SchedulerEventType.OPERATION_RESUME_DENIED in seen_events
    assert SchedulerEventType.OPERATION_MANAGER_RESUMED not in seen_events


def test_scheduler_sync_request_callback_timeout_denies_cancel_request() -> None:
    def request_and_notification_callback(event) -> bool | None:
        if event.event_type is SchedulerEventType.OPERATION_CANCEL_REQUESTED:
            time.sleep(0.05)
            return True
        return None

    scheduler = Scheduler(
        agent_id="agent-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
        request_event_timeout_seconds=0.01,
    )
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    cancelled = scheduler.cancel(operation.id)

    assert cancelled is None
    assert operation.lifecycle_status is LifecycleStatus.QUEUED
