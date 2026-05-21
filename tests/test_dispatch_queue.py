from datetime import datetime, timedelta, timezone

import pytest

from operation_dispatcher.dispatch_queue import (
    DispatchQueue,
    SortDirection,
    SortField,
    SortRule,
)
from operation_dispatcher.models import ScheduledOperation


def _scheduled_operation(
    *,
    resource_id: str = "resource-a",
    priority: int = 0,
    release_date: datetime | None = None,
    due_date: datetime | None = None,
    planned_duration: timedelta | None = None,
) -> ScheduledOperation:
    return ScheduledOperation(
        payload={},
        resource_id=resource_id,
        priority=priority,
        release_date=release_date,
        due_date=due_date,
        planned_duration=planned_duration,
    )


def test_add_and_next_returns_first_operation() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    operation = _scheduled_operation(resource_id="resource-a")

    queue.add(operation)
    pulled = queue.next()

    assert pulled is operation
    assert queue.pulled_operation is operation
    assert len(queue) == 0


def test_default_sort_orders_by_release_date_then_priority() -> None:
    now = datetime.now(timezone.utc)
    late_high = _scheduled_operation(
        priority=10,
        release_date=now + timedelta(minutes=10),
    )
    early_low = _scheduled_operation(
        priority=1,
        release_date=now + timedelta(minutes=5),
    )

    queue = DispatchQueue(resource_id="resource-a")
    queue.add(late_high)
    queue.add(early_low)

    assert queue.next() is early_low


def test_default_sort_uses_priority_when_release_date_equal() -> None:
    now = datetime.now(timezone.utc)
    low = _scheduled_operation(priority=1, release_date=now)
    high = _scheduled_operation(priority=10, release_date=now)

    queue = DispatchQueue(resource_id="resource-a")
    queue.add(low)
    queue.add(high)

    assert queue.next() is high


def test_priority_then_release_date_sort_rules() -> None:
    now = datetime.now(timezone.utc)
    high_later = _scheduled_operation(
        priority=10,
        release_date=now + timedelta(minutes=10),
    )
    low_earlier = _scheduled_operation(
        priority=1,
        release_date=now + timedelta(minutes=1),
    )

    queue = DispatchQueue(
        resource_id="resource-a",
        sort_rules=[
            SortRule(field=SortField.PRIORITY, direction=SortDirection.DESC),
            SortRule(field=SortField.RELEASE_DATE, direction=SortDirection.ASC),
        ],
    )
    queue.add(low_earlier)
    queue.add(high_later)

    assert queue.next() is high_later


def test_sort_rules_apply_created_at_fifo_when_other_keys_equal() -> None:
    first = _scheduled_operation(priority=5, release_date=None)
    second = _scheduled_operation(priority=5, release_date=None)

    queue = DispatchQueue(
        resource_id="resource-a",
        sort_rules=[
            SortRule(field=SortField.PRIORITY, direction=SortDirection.DESC),
            SortRule(field=SortField.RELEASE_DATE, direction=SortDirection.ASC),
        ],
    )
    queue.add(second)
    queue.add(first)

    assert queue.next() is first


def test_sort_rules_allow_custom_release_date_precedence() -> None:
    now = datetime.now(timezone.utc)
    high_late = _scheduled_operation(
        priority=10, release_date=now + timedelta(minutes=2)
    )
    low_early = _scheduled_operation(
        priority=1, release_date=now + timedelta(minutes=1)
    )

    queue = DispatchQueue(
        resource_id="resource-a",
        sort_rules=[
            SortRule(field=SortField.RELEASE_DATE, direction=SortDirection.ASC),
            SortRule(field=SortField.PRIORITY, direction=SortDirection.DESC),
        ],
    )
    queue.add(high_late)
    queue.add(low_early)

    assert queue.next() is low_early


def test_sort_rules_reject_empty_configuration() -> None:
    with pytest.raises(ValueError, match="sort_rules must contain at least one rule"):
        DispatchQueue(resource_id="resource-a", sort_rules=[])


def test_resource_id_mismatch_is_rejected() -> None:
    queue = DispatchQueue(resource_id="resource-a")

    with pytest.raises(ValueError, match="resource_id"):
        queue.add(_scheduled_operation(resource_id="resource-b"))


def test_init_with_operations_applies_validation() -> None:
    valid = _scheduled_operation(resource_id="resource-a")
    invalid = _scheduled_operation(resource_id="resource-b")

    with pytest.raises(ValueError, match="resource_id"):
        DispatchQueue(resource_id="resource-a", operations=[valid, invalid])


def test_next_raises_when_operation_already_pulled() -> None:
    first = _scheduled_operation()
    second = _scheduled_operation()

    queue = DispatchQueue(resource_id="resource-a", operations=[first, second])

    assert queue.next() is first
    with pytest.raises(RuntimeError, match="another operation is active"):
        queue.next()


def test_complete_requires_currently_pulled_operation() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    operation = _scheduled_operation()
    queue.add(operation)

    with pytest.raises(ValueError, match="must be pulled"):
        queue.complete(operation)


def test_complete_archives_operation_and_clears_pulled_state() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    operation = _scheduled_operation()
    queue.add(operation)
    pulled = queue.next()

    queue.complete(operation)

    assert pulled is operation
    assert queue.pulled_operation is None
    assert queue.completed_operations == [operation]


def test_cancel_queued_operation_moves_to_completed_history() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    operation = _scheduled_operation()
    queue.add(operation)

    cancelled = queue.cancel(operation.id)

    assert cancelled is operation
    assert queue.get(operation.id) is operation
    assert queue.completed_operations == [operation]
    assert len(queue) == 0


def test_cancel_pulled_operation_clears_active_and_archives() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    operation = _scheduled_operation()
    queue.add(operation)
    queue.next()

    cancelled = queue.cancel(operation.id)

    assert cancelled is operation
    assert queue.pulled_operation is None
    assert queue.completed_operations == [operation]


def test_history_returns_most_recent_first_and_respects_limit() -> None:
    first = _scheduled_operation()
    second = _scheduled_operation()

    queue = DispatchQueue(resource_id="resource-a", operations=[first, second])

    pulled_first = queue.next()
    assert pulled_first is first
    queue.complete(first)

    pulled_second = queue.next()
    assert pulled_second is second
    queue.complete(second)

    history = queue.history()
    limited_history = queue.history(limit=1)

    assert history == [second, first]
    assert limited_history == [second]


def test_get_finds_queued_pulled_and_completed() -> None:
    queued = _scheduled_operation()
    pulled = _scheduled_operation()
    completed = _scheduled_operation()

    queue = DispatchQueue(
        resource_id="resource-a", operations=[queued, pulled, completed]
    )

    next_operation = queue.next()
    assert next_operation is queued
    queue.complete(queued)

    active = queue.next()
    assert active is pulled

    completed_cancelled = queue.cancel(completed.id)
    assert completed_cancelled is completed

    assert queue.get(pulled.id) is pulled
    assert queue.get(queued.id) is queued
    assert queue.get(completed.id) is completed


def test_peek_and_list_and_remove_behaviour() -> None:
    first = _scheduled_operation(priority=1)
    second = _scheduled_operation(priority=2)

    queue = DispatchQueue(resource_id="resource-a")
    queue.add(first)
    queue.add(second)

    listed = queue.list()
    assert listed[0] is second
    assert queue.peek() is second

    removed = queue.remove(first.id)
    assert removed is first
    assert queue.remove(first.id) is None


def test_clear_and_clear_history_only_affect_respective_buckets() -> None:
    queue = DispatchQueue(resource_id="resource-a")
    queued = _scheduled_operation()
    completed = _scheduled_operation()

    queue.add(queued)
    queue.add(completed)
    queue.cancel(completed.id)

    queue.clear()
    assert len(queue) == 0
    assert queue.completed_operations == [completed]

    queue.clear_history()
    assert queue.completed_operations == []


def test_scheduled_operation_rejects_invalid_duration() -> None:
    with pytest.raises(ValueError, match="planned_duration must be > 0"):
        _scheduled_operation(planned_duration=timedelta(0))


def test_scheduled_operation_rejects_invalid_due_date_order() -> None:
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError, match="due_date must be after release_date"):
        _scheduled_operation(
            release_date=now + timedelta(minutes=2),
            due_date=now + timedelta(minutes=1),
        )
