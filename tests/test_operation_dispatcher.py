import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

from operation_dispatcher import (
    EventType,
    ExecutionOutcome,
    OperationExecution,
    ExecutionState,
    OperationDispatcher,
    OperationHistory,
    OperationHistoryEntry,
    OperationDispatcherState,
    ScheduledOperation,
    TerminationReason,
)

DispatcherEventType = EventType


def _scheduled_operation(
    *,
    resource_id: str = "resource-a",
    release_date: datetime | None = None,
    priority: int = 0,
    planned_duration: int | None = None,
) -> ScheduledOperation:
    return ScheduledOperation(
        payload={},
        resource_id=resource_id,
        release_date=release_date,
        priority=priority,
        planned_duration=planned_duration,
    )


def test_dispatcher_starts_and_completes_current_operation() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    started = dispatcher._start_next()

    assert started is scheduled_operation
    assert dispatcher.current_scheduled_operation is scheduled_operation

    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is not None
    assert execution.state is ExecutionState.RUNNING
    assert execution.outcome is ExecutionOutcome.NONE
    assert execution.termination_reason is TerminationReason.NONE

    completed = dispatcher.complete_current()
    assert completed is scheduled_operation
    assert dispatcher.current_scheduled_operation is None

    execution_after_complete = dispatcher.get_execution(scheduled_operation.id)
    assert execution_after_complete is not None
    assert execution_after_complete.state is ExecutionState.COMPLETED
    assert execution_after_complete.outcome is ExecutionOutcome.SUCCESS
    assert execution_after_complete.finish_time is not None


def test_dispatcher_add_reuses_provided_execution() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    existing_execution = OperationExecution(
        operation_id=scheduled_operation.id,
        state=ExecutionState.PAUSED,
    )

    dispatcher.add(scheduled_operation, execution=existing_execution)

    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is existing_execution
    assert execution is not None
    assert execution.state is ExecutionState.PAUSED


def test_dispatcher_add_applies_default_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    scheduled_operation = _scheduled_operation(planned_duration=None)

    dispatcher.add(scheduled_operation)

    added = dispatcher.get_scheduled_operation(scheduled_operation.id)
    assert added is not None
    assert added.planned_duration == 500


def test_dispatcher_add_keeps_explicit_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    scheduled_operation = _scheduled_operation(planned_duration=1200)

    dispatcher.add(scheduled_operation)

    added = dispatcher.get_scheduled_operation(scheduled_operation.id)
    assert added is not None
    assert added.planned_duration == 1200


def test_dispatcher_add_can_skip_default_planned_duration() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        default_planned_duration=500,
    )
    scheduled_operation = _scheduled_operation(planned_duration=None)

    dispatcher.add(scheduled_operation, apply_default_planned_duration=False)

    added = dispatcher.get_scheduled_operation(scheduled_operation.id)
    assert added is not None
    assert added.planned_duration is None


def test_dispatcher_rejects_non_positive_default_planned_duration() -> None:
    try:
        OperationDispatcher(resource_id="resource-a", default_planned_duration=0)
        assert False, "expected ValueError"
    except ValueError as error:
        assert str(error) == "default_planned_duration must be > 0"


def test_dispatcher_add_with_provided_execution_emits_added_event_with_execution_id() -> (
    None
):
    seen_events = []

    def notification_callback(event) -> None:
        seen_events.append(event)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_notification_callback=notification_callback,
    )
    scheduled_operation = _scheduled_operation()
    existing_execution = OperationExecution(operation_id=scheduled_operation.id)

    dispatcher.add(scheduled_operation, execution=existing_execution)

    assert len(seen_events) == 1
    added_event = seen_events[0]
    assert added_event.event_type is DispatcherEventType.OPERATION_ADDED
    assert added_event.operation_id == scheduled_operation.id
    assert added_event.execution_id == existing_execution.id


def test_dispatcher_add_normalizes_created_at_to_utc() -> None:
    seen_events = []

    def notification_callback(event) -> None:
        seen_events.append(event)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_notification_callback=notification_callback,
    )
    scheduled_operation = ScheduledOperation(
        payload={},
        resource_id="resource-a",
        created_at=datetime(2026, 5, 25, 10, 0, tzinfo=timezone(timedelta(hours=2))),
    )

    dispatcher.add(scheduled_operation)

    assert scheduled_operation.created_at.tzinfo is timezone.utc
    assert len(seen_events) == 1
    added_event = seen_events[0]
    assert added_event.event_type is DispatcherEventType.OPERATION_ADDED
    assert added_event.created_at.tzinfo is timezone.utc


def test_dispatcher_run_once_starts_operation_when_requests_allowed() -> None:
    seen_events: list[DispatcherEventType] = []

    def request_and_notification_callback(event) -> bool | None:
        seen_events.append(event.event_type)
        return True

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=request_and_notification_callback,
        on_notification_callback=request_and_notification_callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    executed = asyncio.run(dispatcher.run_once())

    assert executed is scheduled_operation
    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_START_REQUESTED,
        DispatcherEventType.OPERATION_STARTED,
    ]


def test_dispatcher_pause_blocks_start_until_resumed() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    dispatcher.pause_dispatcher_runtime()
    assert dispatcher._start_next() is None

    dispatcher.resume_dispatcher_runtime()
    assert dispatcher._start_next() is scheduled_operation


def test_dispatcher_cancel_queued_operation_sets_execution_cancelled() -> None:
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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    cancelled = dispatcher.cancel(scheduled_operation.id)

    assert cancelled is scheduled_operation
    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is not None
    assert execution.state is ExecutionState.CANCELLED
    assert execution.outcome is ExecutionOutcome.CANCELLED
    assert execution.termination_reason is TerminationReason.USER_REQUEST
    assert execution.finish_time is not None
    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_CANCEL_REQUESTED,
        DispatcherEventType.OPERATION_CANCELLED,
    ]


def test_dispatcher_cancel_current_operation_sets_execution_cancelled() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    cancelled = dispatcher.cancel(scheduled_operation.id)

    assert cancelled is scheduled_operation
    assert dispatcher.current_scheduled_operation is None
    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is not None
    assert execution.state is ExecutionState.CANCELLED


def test_dispatcher_fail_current_sets_failed_execution() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    failed = dispatcher.fail_current()

    assert failed is scheduled_operation
    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is not None
    assert execution.state is ExecutionState.FAILED
    assert execution.outcome is ExecutionOutcome.FAILURE
    assert execution.termination_reason is TerminationReason.INTERNAL_ERROR
    assert execution.finish_time is not None


def test_dispatcher_run_once_waits_for_release_date() -> None:
    future_release = datetime.now(timezone.utc) + timedelta(minutes=2)
    scheduled_operation = _scheduled_operation(release_date=future_release)
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher.add(scheduled_operation)

    executed = asyncio.run(dispatcher.run_once())

    assert executed is None
    assert dispatcher.current_scheduled_operation is None
    execution = dispatcher.get_execution(scheduled_operation.id)
    assert execution is not None
    assert execution.state is ExecutionState.QUEUED


def test_dispatcher_get_state_reports_runtime_and_queue_information() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    initial_state = dispatcher.get_state()
    assert isinstance(initial_state, OperationDispatcherState)
    assert initial_state.is_running is False
    assert initial_state.queue_size == 1
    assert initial_state.current_operation is None

    dispatcher._start_next()
    running_state = dispatcher.get_state()
    assert running_state.current_operation is not None
    assert running_state.current_operation.id == scheduled_operation.id


def test_dispatcher_emits_lifecycle_events() -> None:
    seen_events: list[DispatcherEventType] = []

    def notification_callback(event) -> None:
        seen_events.append(event.event_type)

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_notification_callback=notification_callback,
    )
    scheduled_operation = _scheduled_operation()

    dispatcher.add(scheduled_operation)
    dispatcher._start_next()
    dispatcher.complete_current()

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


def test_dispatcher_pause_current_emits_pause_requested_before_paused() -> None:
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_PAUSE_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    paused = dispatcher.pause_current_operation()
    assert paused is True

    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
        DispatcherEventType.OPERATION_STARTED,
        DispatcherEventType.OPERATION_PAUSE_REQUESTED,
        DispatcherEventType.OPERATION_PAUSED,
    ]


def test_dispatcher_resume_emits_resume_requested_when_current_exists() -> None:
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_RESUME_REQUESTED:
            return True
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    dispatcher.pause_dispatcher_runtime()
    dispatcher.resume_dispatcher_runtime()
    resume_accepted = dispatcher.resume_current_operation()

    assert resume_accepted is True

    assert seen_events == [
        DispatcherEventType.OPERATION_ADDED,
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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    executed = asyncio.run(dispatcher.run_once())

    assert executed is None
    assert dispatcher.current_scheduled_operation is None
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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    async def run_attempts() -> ScheduledOperation | None:
        first = await dispatcher.run_once()
        second = await dispatcher.run_once()
        await asyncio.sleep(0.03)
        third = await dispatcher.run_once()

        assert first is None
        assert second is None
        return third

    executed = asyncio.run(run_attempts())

    assert executed is scheduled_operation
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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    first = asyncio.run(dispatcher.run_once())
    second = asyncio.run(dispatcher.run_once())

    assert first is None
    assert second is None
    assert dispatcher.is_paused is True
    assert dispatcher.current_scheduled_operation is None
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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    executed = asyncio.run(dispatcher.run_once())

    assert executed is scheduled_operation
    request_events = [
        event
        for event in dispatcher.get_event_history()
        if event.event_type is DispatcherEventType.OPERATION_START_REQUESTED
    ]
    assert len(request_events) == 1
    request_event = request_events[0]
    assert request_event.event_type is DispatcherEventType.OPERATION_START_REQUESTED
    assert request_event.payload["request_decision"]["accepted"] is True
    assert request_event.payload["request_decision"]["reasoning"] == "policy_allowed"
    assert request_event.payload["request_decision"]["data"] == {"rule": "start_ok"}


def test_dispatcher_history_entries_include_execution_and_events() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()
    dispatcher.complete_current()

    history = dispatcher.get_history(limit=1)

    assert isinstance(history, OperationHistory)
    assert history.number_of_entries == 1
    assert len(history.entries) == 1
    history_entry = history.entries[0]
    assert isinstance(history_entry, OperationHistoryEntry)
    assert history_entry.scheduled_operation.id == scheduled_operation.id
    assert len(history_entry.execution) == 1
    assert history_entry.execution[0].state is ExecutionState.COMPLETED
    assert any(
        event.event_type is DispatcherEventType.OPERATION_STARTED
        for event in history_entry.events
    )
    assert any(
        event.event_type is DispatcherEventType.OPERATION_COMPLETED
        for event in history_entry.events
    )


def test_dispatcher_history_callback_can_merge_external_history() -> None:
    callback_calls: list[tuple[int | None, OperationHistory]] = []

    def history_callback(
        limit: int | None,
        in_memory_history: OperationHistory,
    ) -> OperationHistory:
        callback_calls.append((limit, in_memory_history))

        external_entry = OperationHistoryEntry(
            scheduled_operation=ScheduledOperation(
                payload={"task": "external"},
                resource_id="resource-a",
            ),
            execution=[
                OperationExecution(
                    operation_id=UUID(int=1),
                    state=ExecutionState.COMPLETED,
                    outcome=ExecutionOutcome.SUCCESS,
                )
            ],
            events=[],
        )

        merged_entries = [external_entry, *in_memory_history.entries]
        return OperationHistory(
            number_of_entries=len(merged_entries),
            entries=merged_entries,
        )

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_history_callback=history_callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()
    dispatcher.complete_current()

    history = dispatcher.get_history(limit=1)

    assert len(callback_calls) == 1
    callback_limit, callback_in_memory_history = callback_calls[0]
    assert callback_limit == 1
    assert callback_in_memory_history.number_of_entries == 1
    assert history.number_of_entries == 2
    assert history.entries[0].scheduled_operation.payload["task"] == "external"


def test_dispatcher_history_callback_none_falls_back_to_in_memory_history() -> None:
    def history_callback(
        limit: int | None,
        in_memory_history: OperationHistory,
    ) -> None:
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_history_callback=history_callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()
    dispatcher.complete_current()

    history = dispatcher.get_history(limit=1)

    assert history.number_of_entries == 1
    assert len(history.entries) == 1


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
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)

    executed = asyncio.run(dispatcher.run_once())

    assert executed is None
    denied_events = [
        event
        for event in seen_events
        if event.event_type is DispatcherEventType.OPERATION_START_DENIED
    ]
    assert len(denied_events) == 1
    denied_event = denied_events[0]
    assert denied_event.payload["reasoning"] == "resource_busy"
    assert denied_event.payload["decision_data"] == {"resource_state": "busy"}


def test_dispatcher_does_not_pause_when_pause_request_denied() -> None:
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_PAUSE_REQUESTED:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    paused = dispatcher.pause_current_operation()

    assert paused is False
    assert dispatcher.current_scheduled_operation is scheduled_operation
    assert DispatcherEventType.OPERATION_PAUSE_REQUESTED in seen_events
    assert DispatcherEventType.OPERATION_PAUSE_DENIED in seen_events
    assert DispatcherEventType.OPERATION_PAUSED not in seen_events


def test_dispatcher_does_not_resume_when_resume_request_denied() -> None:
    seen_events: list[DispatcherEventType] = []

    def callback(event) -> bool | None:
        seen_events.append(event.event_type)
        if event.event_type is DispatcherEventType.OPERATION_RESUME_REQUESTED:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        on_request_callback=callback,
        on_notification_callback=callback,
    )
    scheduled_operation = _scheduled_operation()
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()
    dispatcher.pause_dispatcher_runtime()

    dispatcher.resume_dispatcher_runtime()
    resume_accepted = dispatcher.resume_current_operation()

    assert resume_accepted is False
    assert dispatcher.is_paused is False
    assert DispatcherEventType.OPERATION_RESUME_REQUESTED in seen_events
    assert DispatcherEventType.OPERATION_RESUME_DENIED in seen_events
    assert DispatcherEventType.OPERATION_DISPATCHER_RESUMED in seen_events
    assert DispatcherEventType.OPERATION_RESUMED not in seen_events
