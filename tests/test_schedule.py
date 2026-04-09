from datetime import datetime, timedelta, timezone

import pytest

from operation_scheduler import Operation, Schedule, TimedOperation


def test_add_and_get_next_operation() -> None:
    schedule = Schedule()
    operation = Operation(name="sync", agent_id="agent-a")

    schedule.add(operation)
    next_operation = schedule.next()

    assert next_operation is not None
    assert next_operation.name == "sync"
    assert len(schedule) == 0


def test_priority_ordering() -> None:
    schedule = Schedule()
    low = Operation(name="low", agent_id="agent-a", priority=1)
    high = Operation(name="high", agent_id="agent-a", priority=10)

    schedule.add(low)
    schedule.add(high)

    assert schedule.next() == high
    assert schedule.next() == low


def test_timed_operation_has_planned_and_actual_times() -> None:
    now = datetime.now(timezone.utc)
    timed_operation = TimedOperation(
        name="timed-sync",
        agent_id="agent-a",
        planned_start_time=now,
        planned_finish_time=now + timedelta(minutes=30),
        actual_start_time=now + timedelta(minutes=1),
        actual_finish_time=now + timedelta(minutes=28),
    )

    assert timed_operation.planned_start_time == now
    assert timed_operation.actual_finish_time == now + timedelta(minutes=28)


def test_timed_operation_rejects_invalid_planned_time_range() -> None:
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError):
        TimedOperation(
            name="invalid",
            agent_id="agent-a",
            planned_start_time=now,
            planned_finish_time=now - timedelta(minutes=1),
        )


def test_schedule_orders_timed_operations_by_planned_start_time() -> None:
    now = datetime.now(timezone.utc)
    first = TimedOperation(
        name="first",
        agent_id="agent-a",
        planned_start_time=now + timedelta(minutes=10),
        planned_finish_time=now + timedelta(minutes=20),
        priority=1,
    )
    second = TimedOperation(
        name="second",
        agent_id="agent-a",
        planned_start_time=now + timedelta(minutes=5),
        planned_finish_time=now + timedelta(minutes=15),
        priority=1,
    )

    schedule = Schedule(operation_class=TimedOperation)
    schedule.add(first)
    schedule.add(second)

    assert schedule.next() == second
    assert schedule.next() == first


def test_schedule_sets_running_status_on_next_for_timed_operations() -> None:
    now = datetime.now(timezone.utc)
    operation = TimedOperation(
        name="timed",
        agent_id="agent-a",
        planned_start_time=now,
        planned_finish_time=now + timedelta(minutes=10),
    )
    schedule = Schedule([operation], operation_class=TimedOperation)

    next_operation = schedule.next()

    assert next_operation is not None
    assert next_operation.status.value == "running"


def test_schedule_with_timed_operation_class_orders_by_planned_start_time() -> None:
    now = datetime.now(timezone.utc)
    later = TimedOperation(
        name="later",
        agent_id="agent-a",
        planned_start_time=now + timedelta(minutes=20),
        planned_finish_time=now + timedelta(minutes=30),
        priority=100,
    )
    earlier = TimedOperation(
        name="earlier",
        agent_id="agent-a",
        planned_start_time=now + timedelta(minutes=10),
        planned_finish_time=now + timedelta(minutes=15),
        priority=1,
    )

    schedule = Schedule(operation_class=TimedOperation)
    schedule.add(later)
    schedule.add(earlier)

    assert schedule.next() == earlier


def test_schedule_rejects_wrong_operation_type() -> None:
    now = datetime.now(timezone.utc)
    schedule = Schedule(operation_class=TimedOperation)

    with pytest.raises(TypeError):
        schedule.add(Operation(name="normal", agent_id="agent-a"))

    timed = TimedOperation(
        name="timed",
        agent_id="agent-a",
        planned_start_time=now,
        planned_finish_time=now + timedelta(minutes=5),
    )
    schedule.add(timed)
    assert schedule.next() == timed


def test_schedule_rejects_wrong_operation_type_in_init() -> None:
    with pytest.raises(TypeError):
        Schedule(
            operations=[Operation(name="normal", agent_id="agent-a")],
            operation_class=TimedOperation,
        )


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


def test_schedule_rejects_timed_operation_for_different_agent() -> None:
    now = datetime.now(timezone.utc)
    timed_operation = TimedOperation(
        name="timed-sync",
        agent_id="agent-b",
        planned_start_time=now,
        planned_finish_time=now + timedelta(minutes=10),
    )

    schedule = Schedule(operation_class=TimedOperation, agent_id="agent-a")
    with pytest.raises(ValueError):
        schedule.add(timed_operation)


def test_schedule_agent_id_property_is_exposed() -> None:
    schedule = Schedule(agent_id="agent-a")
    timed_schedule = Schedule(operation_class=TimedOperation, agent_id="agent-b")

    assert schedule.agent_id == "agent-a"
    assert timed_schedule.agent_id == "agent-b"


def test_schedule_tracks_pulled_and_completed_operations() -> None:
    schedule = Schedule()
    operation = Operation(name="sync", agent_id="agent-a")
    schedule.add(operation)

    pulled = schedule.next()
    assert pulled is operation
    assert schedule.pulled_operations == [operation]
    assert schedule.completed_operations == []

    schedule.complete(operation)
    assert schedule.completed_operations == [operation]
    assert operation.status.value == "completed"


def test_schedule_complete_requires_pulled_operation() -> None:
    schedule = Schedule()
    operation = Operation(name="sync", agent_id="agent-a")
    schedule.add(operation)

    with pytest.raises(ValueError):
        schedule.complete(operation)


def test_timed_operation_actual_times_are_set_on_pull_and_complete() -> None:
    now = datetime.now(timezone.utc)
    operation = TimedOperation(
        name="timed",
        agent_id="agent-a",
        planned_start_time=now,
        planned_finish_time=now + timedelta(minutes=10),
    )
    schedule = Schedule(operation_class=TimedOperation)
    schedule.add(operation)

    pulled = schedule.next()
    assert pulled is operation
    assert operation.actual_start_time is not None
    assert operation.actual_finish_time is None

    schedule.complete(operation)
    assert operation.actual_finish_time is not None
