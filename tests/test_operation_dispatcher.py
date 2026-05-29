import asyncio
from datetime import datetime, timedelta, timezone

from operation_dispatcher import (
    EventType,
    ExecutionOutcome,
    ExecutionState,
    History,
    HistoryRecord,
    Operation,
    OperationDispatcher,
    OperationDispatcherState,
    TerminationReason,
)

DispatcherEventType = EventType


def _operation(
    *,
    resource_id: str = "resource-a",
    release_date: datetime | None = None,
    priority: int = 0,
    planned_duration: int | None = None,
) -> Operation:
    return Operation(
        payload={},
        resource_id=resource_id,
        release_date=release_date,
        priority=priority,
        planned_duration=planned_duration,
    )


def test_dispatcher_starts_and_completes_operation() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)

    started = asyncio.run(dispatcher.step_dispatch())

    assert started is operation
    assert dispatcher.current_operation is operation
    assert operation.state is ExecutionState.RUNNING
    assert operation.outcome is ExecutionOutcome.NONE
    assert operation.termination_reason is TerminationReason.NONE

    completed = dispatcher.complete_operation(operation.id)

    assert completed is operation
    assert dispatcher.current_operation is None
    assert operation.state is ExecutionState.COMPLETED
    assert operation.outcome is ExecutionOutcome.SUCCESS
    assert operation.finish_time is not None


def test_dispatcher_add_applies_default_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    operation = _operation(planned_duration=None)

    dispatcher.add_operation(operation)

    added = dispatcher.get_operation(operation.id)
    assert added is not None
    assert added.planned_duration == 500


def test_dispatcher_add_keeps_explicit_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    operation = _operation(planned_duration=1200)

    dispatcher.add_operation(operation)

    added = dispatcher.get_operation(operation.id)
    assert added is not None
    assert added.planned_duration == 1200


def test_dispatcher_add_can_skip_default_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    operation = _operation(planned_duration=None)

    dispatcher.add_operation(operation, apply_default_planned_duration=False)

    added = dispatcher.get_operation(operation.id)
    assert added is not None
    assert added.planned_duration is None


def test_dispatcher_rejects_non_positive_default_planned_duration() -> None:
    try:
        OperationDispatcher(resource_id="resource-a", default_planned_duration=0)
        assert False, "expected ValueError"
    except ValueError as error:
        assert str(error) == "default_planned_duration must be > 0"


def test_dispatcher_add_normalizes_created_at_to_utc() -> None:
    seen_events = []

    def notification_callback(event) -> None:
        seen_events.append(event)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_notification_callback=notification_callback,
    )
    operation = Operation(
        payload={},
        resource_id="resource-a",
        created_at=datetime(2026, 5, 25, 10, 0, tzinfo=timezone(timedelta(hours=2))),
    )

    dispatcher.add_operation(operation)

    assert operation.created_at.tzinfo is timezone.utc
    assert len(seen_events) == 1
    assert seen_events[0].event_type is DispatcherEventType.OPERATION_ADDED


def test_dispatcher_step_dispatch_starts_operation_when_requests_allowed() -> None:
    seen_events: list[DispatcherEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        return True

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    executed = asyncio.run(dispatcher.step_dispatch())

    assert executed is operation
    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_STARTED,
    ]


def test_dispatcher_pause_blocks_step_dispatch_until_resumed() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)

    dispatcher.pause_dispatcher_runtime()
    assert asyncio.run(dispatcher.step_dispatch()) is None

    dispatcher.resume_dispatcher_runtime()
    assert asyncio.run(dispatcher.step_dispatch()) is operation


def test_dispatcher_cancel_queued_operation_sets_cancelled_state() -> None:
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_CANCEL_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    cancelled = dispatcher.cancel_operation(operation.id)

    assert cancelled is operation
    assert operation.state is ExecutionState.CANCELLED
    assert operation.outcome is ExecutionOutcome.CANCELLED
    assert operation.termination_reason is TerminationReason.INTERNAL_ERROR
    assert operation.finish_time is not None
    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_CANCEL_REQUESTED,
        DispatcherEventType.OPERATION_CANCELLED,
    ]


def test_dispatcher_cancel_current_operation_sets_cancelled_state() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    cancelled = dispatcher.cancel_operation(operation.id)

    assert cancelled is operation
    assert dispatcher.current_operation is None
    assert operation.state is ExecutionState.CANCELLED


def test_dispatcher_fail_operation_sets_failed_state() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    failed = dispatcher.fail_operation(operation.id)

    assert failed is operation
    assert operation.state is ExecutionState.FAILED
    assert operation.outcome is ExecutionOutcome.FAILURE
    assert operation.termination_reason is TerminationReason.INTERNAL_ERROR
    assert operation.finish_time is not None


def test_dispatcher_cancel_operation_accepts_termination_reason_override() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)

    cancelled = dispatcher.cancel_operation(
        operation.id,
        termination_reason=TerminationReason.USER_REQUEST,
    )

    assert cancelled is operation
    assert operation.state is ExecutionState.CANCELLED
    assert operation.outcome is ExecutionOutcome.CANCELLED
    assert operation.termination_reason is TerminationReason.USER_REQUEST


def test_dispatcher_fail_operation_accepts_termination_reason_override() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    failed = dispatcher.fail_operation(
        operation.id,
        termination_reason=TerminationReason.EXTERNAL_ERROR,
    )

    assert failed is operation
    assert operation.state is ExecutionState.FAILED
    assert operation.outcome is ExecutionOutcome.FAILURE
    assert operation.termination_reason is TerminationReason.EXTERNAL_ERROR


def test_dispatcher_lifecycle_methods_propagate_meta_data_to_events() -> None:
    def callback(event) -> bool | None:
        if event.event_type is DispatcherEventType.OPERATION_CANCEL_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
    )
    operation = _operation()
    add_meta = {"source": "api", "trace_id": "op-123"}
    update_meta = {"source": "api", "action": "update"}
    cancel_meta = {"source": "api", "action": "cancel", "reason": "manual"}

    dispatcher.add_operation(operation, meta_data=add_meta)
    dispatcher.update_operation(
        operation.id,
        {"priority": 9},
        meta_data=update_meta,
    )
    dispatcher.cancel_operation(operation.id, meta_data=cancel_meta)

    event_by_type = {
        event.event_type: event for event in dispatcher.get_event_history()
    }

    assert event_by_type[DispatcherEventType.OPERATION_ADDED].meta_data == add_meta
    assert event_by_type[DispatcherEventType.OPERATION_UPDATED].meta_data == update_meta

    cancel_requested_event = event_by_type[
        DispatcherEventType.OPERATION_CANCEL_REQUESTED
    ]
    assert cancel_requested_event.meta_data["source"] == "api"
    assert cancel_requested_event.meta_data["action"] == "cancel"
    assert cancel_requested_event.meta_data["reason"] == "manual"
    assert cancel_requested_event.meta_data["request_decision"]["accepted"] is True

    assert (
        event_by_type[DispatcherEventType.OPERATION_CANCELLED].meta_data == cancel_meta
    )


def test_dispatcher_step_dispatch_waits_for_release_date() -> None:
    future_release = datetime.now(timezone.utc) + timedelta(minutes=2)
    operation = _operation(release_date=future_release)
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher.add_operation(operation)

    executed = asyncio.run(dispatcher.step_dispatch())

    assert executed is None
    assert dispatcher.current_operation is None
    assert operation.state is ExecutionState.QUEUED


def test_dispatcher_get_state_reports_runtime_and_queue_information() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)

    initial_state = dispatcher.get_state()
    assert isinstance(initial_state, OperationDispatcherState)
    assert initial_state.is_running is False
    assert initial_state.queue_size == 1
    assert initial_state.current_operation is None

    asyncio.run(dispatcher.step_dispatch())
    running_state = dispatcher.get_state()
    assert running_state.current_operation is not None
    assert running_state.current_operation.id == operation.id


def test_dispatcher_emits_lifecycle_events() -> None:
    seen_events: list[DispatcherEventType] = []

    def notification_callback(event) -> None:
        seen_events.append(event.event_type)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_notification_callback=notification_callback,
    )
    operation = _operation()

    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(operation.id)

    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_STARTED,
        DispatcherEventType.OPERATION_COMPLETED,
    ]


def test_dispatcher_records_runtime_lifecycle_events() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a", poll_interval_seconds=0.01
    )

    async def run_dispatcher() -> None:
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.05)
        dispatcher.request_stop()
        await task

    asyncio.run(run_dispatcher())
    event_types = [event.event_type for event in dispatcher.get_event_history()]

    assert DispatcherEventType.OPERATION_DISPATCHER_STARTED in event_types
    assert DispatcherEventType.OPERATION_DISPATCHER_STOPPED in event_types


def test_dispatcher_pause_current_operation_emits_pause_requested_before_paused() -> (
    None
):
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return True
        if event.event_type is DispatcherEventType.OPERATION_PAUSE_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    paused = dispatcher.pause_operation(operation.id)
    assert paused is True

    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_STARTED,
        DispatcherEventType.OPERATION_PAUSE_REQUESTED,
        DispatcherEventType.OPERATION_PAUSED,
    ]


def test_dispatcher_resume_current_operation_emits_resume_requested_when_current_exists() -> (
    None
):
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return True
        if event.event_type is DispatcherEventType.OPERATION_RESUME_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    dispatcher.pause_dispatcher_runtime()
    dispatcher.resume_dispatcher_runtime()
    resume_accepted = dispatcher.resume_operation(operation.id)

    assert resume_accepted is True

    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_STARTED,
        DispatcherEventType.OPERATION_DISPATCHER_PAUSED,
        DispatcherEventType.OPERATION_DISPATCHER_RESUMED,
        DispatcherEventType.OPERATION_RESUME_REQUESTED,
        DispatcherEventType.OPERATION_RESUMED,
    ]


def test_dispatcher_denies_start_when_callback_returns_none() -> None:
    seen_event_types: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    executed = asyncio.run(dispatcher.step_dispatch())

    assert executed is None
    assert dispatcher.current_operation is None
    assert DispatcherEventType.OPERATION_START_REQUESTED in seen_event_types
    assert DispatcherEventType.OPERATION_START_DENIED in seen_event_types


def test_dispatcher_retries_denied_start_after_cooldown() -> None:
    seen_event_types: list[DispatcherEventType] = []
    start_request_calls = 0

    def callback(event) -> bool | None:
        nonlocal start_request_calls
        seen_event_types.append(event.event_type)

        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            start_request_calls += 1
            return start_request_calls >= 2

        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
        start_request_retry_cooldown_seconds=0.02,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    async def run_attempts() -> Operation | None:
        first = await dispatcher.step_dispatch()
        second = await dispatcher.step_dispatch()
        await asyncio.sleep(0.03)
        third = await dispatcher.step_dispatch()

        assert first is None
        assert second is None
        return third

    executed = asyncio.run(run_attempts())

    assert executed is operation
    assert start_request_calls == 2
    assert seen_event_types == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_START_DENIED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_STARTED,
    ]


def test_dispatcher_pauses_when_denied_start_reaches_max_retries() -> None:
    seen_event_types: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_event_types.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
        start_request_max_retries=2,
        start_request_retry_cooldown_seconds=0.0,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    first = asyncio.run(dispatcher.step_dispatch())
    second = asyncio.run(dispatcher.step_dispatch())

    assert first is None
    assert second is None
    assert dispatcher.is_paused is True
    assert dispatcher.current_operation is None
    assert seen_event_types == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_START_DENIED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_START_DENIED,
        DispatcherEventType.OPERATION_DISPATCHER_PAUSED,
    ]


def test_dispatcher_accepts_structured_request_decision() -> None:
    def callback(event) -> dict[str, object] | None:
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return {
                "accepted": True,
                "reasoning": "policy_allowed",
                "data": {"rule": "start_ok"},
            }
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    executed = asyncio.run(dispatcher.step_dispatch())

    assert executed is operation
    request_events = [
        event
        for event in dispatcher.get_event_history()
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED
    ]
    assert len(request_events) == 1
    request_event = request_events[0]
    assert request_event.meta_data["request_decision"]["accepted"] is True
    assert request_event.meta_data["request_decision"]["reasoning"] == "policy_allowed"
    assert request_event.meta_data["request_decision"]["data"] == {"rule": "start_ok"}


def test_dispatcher_history_records_include_operation_and_events() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(operation.id)

    history = dispatcher.get_history(limit=1)

    assert isinstance(history, History)
    assert history.num_records == 1
    assert len(history.records) == 1
    history_record = history.records[0]
    assert isinstance(history_record, HistoryRecord)
    assert history_record.operation.id == operation.id
    assert history_record.operation.state is ExecutionState.COMPLETED
    assert any(
        event.event_type is DispatcherEventType.OPERATION_STARTED
        for event in history_record.events
    )
    assert any(
        event.event_type is DispatcherEventType.OPERATION_COMPLETED
        for event in history_record.events
    )


def test_dispatcher_history_callback_can_merge_external_history() -> None:
    callback_calls: list[tuple[int | None, History]] = []

    def history_callback(limit: int | None, in_memory_history: History) -> History:
        callback_calls.append((limit, in_memory_history))

        external_record = HistoryRecord(
            operation=Operation(
                payload={"task": "external"},
                resource_id="resource-a",
                state=ExecutionState.COMPLETED,
                outcome=ExecutionOutcome.SUCCESS,
                start_time=datetime.now(timezone.utc),
            ),
            events=[],
        )

        merged_records = [external_record, *in_memory_history.records]
        return History(
            num_records=len(merged_records),
            records=merged_records,
        )

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_history_callback=history_callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(operation.id)

    history = dispatcher.get_history(limit=1)

    assert len(callback_calls) == 1
    callback_limit, callback_in_memory_history = callback_calls[0]
    assert callback_limit == 1
    assert callback_in_memory_history.num_records == 1
    assert history.num_records == 2
    assert history.records[0].operation.payload["task"] == "external"


def test_dispatcher_history_callback_none_falls_back_to_in_memory_history() -> None:
    def history_callback(limit: int | None, in_memory_history: History) -> None:
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_history_callback=history_callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(operation.id)

    history = dispatcher.get_history(limit=1)

    assert history.num_records == 1
    assert len(history.records) == 1


def test_dispatcher_denied_event_includes_structured_reason_metadata() -> None:
    seen_events: list = []

    def callback(event) -> dict[str, object] | None:
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return {
                "accepted": False,
                "reasoning": "resource_busy",
                "data": {"resource_state": "busy"},
            }
        return None

    def notification_callback(event) -> None:
        seen_events.append(event)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=notification_callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)

    executed = asyncio.run(dispatcher.step_dispatch())

    assert executed is None
    denied_events = [
        event
        for event in seen_events
        if event.event_type is DispatcherEventType.OPERATION_START_DENIED
    ]
    assert len(denied_events) == 1
    denied_event = denied_events[0]
    assert denied_event.meta_data["reasoning"] == "resource_busy"
    assert denied_event.meta_data["decision_data"] == {"resource_state": "busy"}


def test_dispatcher_does_not_pause_current_operation_when_pause_request_denied() -> (
    None
):
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return True
        if event.event_type is DispatcherEventType.OPERATION_PAUSE_REQUESTED:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    paused = dispatcher.pause_operation(operation.id)

    assert paused is False
    assert dispatcher.current_operation is operation
    assert DispatcherEventType.OPERATION_PAUSE_REQUESTED in seen_events
    assert DispatcherEventType.OPERATION_PAUSE_DENIED in seen_events
    assert DispatcherEventType.OPERATION_PAUSED not in seen_events


def test_dispatcher_does_not_resume_current_operation_when_resume_request_denied() -> (
    None
):
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED:
            return True
        if event.event_type is DispatcherEventType.OPERATION_RESUME_REQUESTED:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    operation = _operation()
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.pause_dispatcher_runtime()

    dispatcher.resume_dispatcher_runtime()
    resume_accepted = dispatcher.resume_operation(operation.id)

    assert resume_accepted is False
    assert dispatcher.is_paused is False
    assert DispatcherEventType.OPERATION_RESUME_REQUESTED in seen_events
    assert DispatcherEventType.OPERATION_RESUME_DENIED in seen_events
    assert DispatcherEventType.OPERATION_DISPATCHER_RESUMED in seen_events
    assert DispatcherEventType.OPERATION_RESUMED not in seen_events


def test_dispatcher_update_operation_emits_operation_updated_with_changes() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    operation = _operation(priority=1)
    dispatcher.add_operation(operation)

    updated = dispatcher.update_operation(operation.id, {"priority": 5})

    assert updated is operation
    assert updated is not None
    assert updated.priority == 5

    updated_events = [
        event
        for event in dispatcher.get_event_history()
        if event.event_type is DispatcherEventType.OPERATION_UPDATED
    ]
    assert len(updated_events) == 1
    assert any(change.field == "priority" for change in updated_events[0].changes)


def test_dispatcher_update_operation_reorders_queue_when_priority_changes() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    higher_priority = _operation(priority=5)
    lower_priority = _operation(priority=1)
    dispatcher.add_operation(higher_priority)
    dispatcher.add_operation(lower_priority)

    schedule_before = dispatcher.get_schedule()
    assert schedule_before[0] is higher_priority
    assert schedule_before[1] is lower_priority

    updated = dispatcher.update_operation(lower_priority.id, {"priority": 10})

    assert updated is lower_priority
    schedule_after = dispatcher.get_schedule()
    assert schedule_after[0] is lower_priority
    assert schedule_after[1] is higher_priority
