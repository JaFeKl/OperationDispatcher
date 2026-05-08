from datetime import datetime, timedelta, timezone

import pytest

from operation_manager import (
    ExecutionOutcome,
    LifecycleStatus,
    Operation,
    Schedule,
    ScheduleSortStrategy,
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

    first = schedule.next()
    assert first == high
    schedule.complete(high)

    second = schedule.next()
    assert second == low


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

    pulled_second = schedule.next()
    assert pulled_second == second
    schedule.complete(second)

    pulled_first = schedule.next()
    assert pulled_first == first


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
    assert schedule.pulled_operation is operation
    assert schedule.completed_operations == []

    schedule.complete(operation)
    assert schedule.pulled_operation is None
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


def test_schedule_rejects_pulling_multiple_operations_without_terminal_transition() -> (
    None
):
    schedule = Schedule(agent_id="agent-a")
    first = Operation(name="first", agent_id="agent-a")
    second = Operation(name="second", agent_id="agent-a")
    schedule.add(first)
    schedule.add(second)

    pulled_first = schedule.next()

    assert pulled_first is first
    with pytest.raises(RuntimeError):
        schedule.next()


def test_schedule_cancel_active_operation_moves_it_to_completed_history() -> None:
    schedule = Schedule(agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    schedule.add(operation)
    schedule.next()

    cancelled = schedule.cancel(operation.id)

    assert cancelled is operation
    assert schedule.pulled_operation is None
    assert operation.lifecycle_status is LifecycleStatus.FINISHED
    assert operation.execution_outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.CANCELLED_DURING_RUN
    assert operation.finish_time is not None
    assert schedule.completed_operations == [operation]


def test_schedule_can_sort_windowed_operations_by_priority_first() -> None:
    now = datetime.now(timezone.utc)
    higher_priority_later_start = Operation(
        name="later-high-priority",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=20),
            end=now + timedelta(minutes=30),
        ),
        priority=100,
    )
    lower_priority_earlier_start = Operation(
        name="earlier-low-priority",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=10),
            end=now + timedelta(minutes=15),
        ),
        priority=1,
    )

    schedule = Schedule(
        agent_id="agent-a",
        sort_strategy=ScheduleSortStrategy.PRIORITY_THEN_START_TIME,
    )
    schedule.add(higher_priority_later_start)
    schedule.add(lower_priority_earlier_start)

    assert schedule.next() == higher_priority_later_start


def test_schedule_can_sort_windowed_operations_by_start_time_first() -> None:
    now = datetime.now(timezone.utc)
    higher_priority_later_start = Operation(
        name="later-high-priority",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=20),
            end=now + timedelta(minutes=30),
        ),
        priority=100,
    )
    lower_priority_earlier_start = Operation(
        name="earlier-low-priority",
        agent_id="agent-a",
        time_window=TimeWindow(
            start=now + timedelta(minutes=10),
            end=now + timedelta(minutes=15),
        ),
        priority=1,
    )

    schedule = Schedule(
        agent_id="agent-a",
        sort_strategy=ScheduleSortStrategy.START_TIME_THEN_PRIORITY,
    )
    schedule.add(higher_priority_later_start)
    schedule.add(lower_priority_earlier_start)

    assert schedule.next() == lower_priority_earlier_start
