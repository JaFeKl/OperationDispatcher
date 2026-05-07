from datetime import datetime, timedelta, timezone

import pytest

from operation_scheduler import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    Schedule,
    TerminationReason,
    TimeWindow,
)


def test_add_and_get_next_operation() -> None:
    schedule = Schedule(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")

    schedule.add(operation)
    next_operation = schedule.next()

    assert next_operation is not None
    assert next_operation.name == "sync"
    assert len(schedule) == 0


def test_priority_ordering() -> None:
    schedule = Schedule(agent_id="agent-a")
    low = Operation(name="low", agent_id="agent-a", priority=1)
    high = Operation(name="high", agent_id="agent-a", priority=10)

    schedule.add(low)
    schedule.add(high)

    assert schedule.next() == high
    assert schedule.next() == low


def test_operation_time_window_and_actual_times() -> None:
    now = datetime.now(timezone.utc)
    operation = Operation(
        name="timed-sync",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now,
            end=now + timedelta(minutes=30),
        ),
        start_time=now + timedelta(minutes=1),
        finish_time=now + timedelta(minutes=28),
    )

    assert operation.time_window is not None
    assert operation.time_window.start == now
    assert operation.finish_time == now + timedelta(minutes=28)


def test_time_window_rejects_invalid_range() -> None:
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError):
        TimeWindow(
            start=now,
            end=now - timedelta(minutes=1),
        )


def test_schedule_orders_operations_with_time_window_by_start_time() -> None:
    now = datetime.now(timezone.utc)
    first = Operation(
        name="first",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=10),
            end=now + timedelta(minutes=20),
        ),
        priority=1,
    )
    second = Operation(
        name="second",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=5),
            end=now + timedelta(minutes=15),
        ),
        priority=1,
    )

    schedule = Schedule(agent_id="agent-a")
    schedule.add(first)
    schedule.add(second)

    assert schedule.next() == second
    assert schedule.next() == first


def test_schedule_sets_running_status_on_next_for_windowed_operations() -> None:
    now = datetime.now(timezone.utc)
    operation = Operation(
        name="timed",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now,
            end=now + timedelta(minutes=10),
        ),
    )
    schedule = Schedule(agent_id="agent-a", operations=[operation])

    next_operation = schedule.next()

    assert next_operation is not None
    assert next_operation.lifecycle_status is LifecycleStatus.RUNNING
    assert next_operation.execution_outcome is ExecutionOutcome.NONE
    assert next_operation.termination_reason is TerminationReason.NONE


def test_schedule_rejects_windowed_operation_after_plain_queue_type_locked() -> None:
    now = datetime.now(timezone.utc)
    schedule = Schedule(agent_id="agent-a")
    schedule.add(Operation(name="plain", agent_id="agent-a", priority=1))

    with pytest.raises(ValueError):
        schedule.add(
            Operation(
                name="windowed",
                agent_id="agent-a",
                time_window=TimeWindow(
                    start=now,
                    end=now + timedelta(minutes=5),
                ),
            )
        )


def test_schedule_rejects_plain_operation_after_windowed_queue_type_locked() -> None:
    now = datetime.now(timezone.utc)
    schedule = Schedule(agent_id="agent-a")
    schedule.add(
        Operation(
            name="windowed",
            agent_id="agent-a",
            time_window=TimeWindow(
                start=now,
                end=now + timedelta(minutes=5),
            ),
        )
    )

    with pytest.raises(ValueError):
        schedule.add(Operation(name="plain", agent_id="agent-a", priority=1))


def test_schedule_rejects_mixed_queue_types_in_init() -> None:
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError):
        Schedule(
            agent_id="agent-a",
            operations=[
                Operation(name="plain", agent_id="agent-a", priority=1),
                Operation(
                    name="windowed",
                    agent_id="agent-a",
                    time_window=TimeWindow(
                        start=now,
                        end=now + timedelta(minutes=5),
                    ),
                ),
            ],
        )


def test_schedule_with_windowed_operations_orders_by_start_then_priority() -> None:
    now = datetime.now(timezone.utc)
    later = Operation(
        name="later",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=20),
            end=now + timedelta(minutes=30),
        ),
        priority=100,
    )
    earlier = Operation(
        name="earlier",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=10),
            end=now + timedelta(minutes=15),
        ),
        priority=1,
    )

    schedule = Schedule(agent_id="agent-a")
    schedule.add(later)
    schedule.add(earlier)

    assert schedule.next() == earlier


def test_schedule_accepts_operation_subclass() -> None:
    class CustomOperation(Operation):
        special: str

    schedule = Schedule(agent_id="agent-a")

    custom = CustomOperation(name="custom", agent_id="agent-a", special="x")
    schedule.add(custom)
    assert schedule.next() == custom


def test_schedule_rejects_operation_for_different_agent() -> None:
    schedule = Schedule(agent_id="agent-a")

    with pytest.raises(ValueError):
        schedule.add(Operation(name="sync", agent_id="agent-b"))


def test_schedule_rejects_different_agent_in_init_operations() -> None:
    with pytest.raises(ValueError):
        Schedule(
            operations=[Operation(name="sync", agent_id="agent-b")],
            agent_id="agent-a",
        )


def test_schedule_rejects_windowed_operation_for_different_agent() -> None:
    now = datetime.now(timezone.utc)
    windowed_operation = Operation(
        name="timed-sync",
        agent_id="agent-b",
        time_window=TimeWindow(
            start=now,
            end=now + timedelta(minutes=10),
        ),
    )

    schedule = Schedule(agent_id="agent-a")
    with pytest.raises(ValueError):
        schedule.add(windowed_operation)


def test_schedule_agent_id_property_is_exposed() -> None:
    schedule = Schedule(agent_id="agent-a")
    timed_schedule = Schedule(agent_id="agent-b")

    assert schedule.agent_id == "agent-a"
    assert timed_schedule.agent_id == "agent-b"


def test_schedule_tracks_pulled_and_completed_operations() -> None:
    schedule = Schedule(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    schedule.add(operation)

    pulled = schedule.next()
    assert pulled is operation
    assert schedule.pulled_operations == [operation]
    assert schedule.completed_operations == []

    schedule.complete(operation)
    assert schedule.completed_operations == [operation]
    assert operation.lifecycle_status is LifecycleStatus.FINISHED
    assert operation.execution_outcome is ExecutionOutcome.SUCCEEDED
    assert operation.termination_reason is TerminationReason.NONE


def test_schedule_complete_requires_pulled_operation() -> None:
    schedule = Schedule(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    schedule.add(operation)

    with pytest.raises(ValueError):
        schedule.complete(operation)


def test_operation_start_and_finish_times_are_set_on_pull_and_complete() -> None:
    now = datetime.now(timezone.utc)
    operation = Operation(
        name="timed",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now,
            end=now + timedelta(minutes=10),
        ),
    )
    schedule = Schedule(agent_id="agent-a")
    schedule.add(operation)

    pulled = schedule.next()
    assert pulled is operation
    assert operation.start_time is not None
    assert operation.finish_time is None

    schedule.complete(operation)
    assert operation.finish_time is not None
