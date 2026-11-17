import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
from uuid import uuid4

import pytest

from operation_dispatcher import (
    EventType,
    ExecutionOutcome,
    ExecutionState,
    Operation,
    OperationDependency,
    OperationDispatcher,
    OperationDispatcherRuntimeController,
    RequestDecision,
)


def add(dispatcher, **kwargs):
    operation = Operation(resource_id="resource-a", **kwargs)
    dispatcher.add_operation(operation)
    return operation


@pytest.mark.parametrize("change", ["priority", "payload", "cancel", "stop"])
def test_start_approval_is_invalidated_by_concurrent_changes(change):
    entered, release = threading.Event(), threading.Event()
    approvals = []

    def approve(event):
        if event.event_type is EventType.OPERATION_START_REQUESTED:
            approvals.append(event.operation_id)
            entered.set()
            assert release.wait(2)
        return True

    dispatcher = OperationDispatcher("resource-a", on_request_callback=approve)
    first = add(dispatcher, priority=10)
    second = add(dispatcher, priority=0)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(asyncio.run, dispatcher.step_dispatch())
        try:
            assert entered.wait(2)
            # A second concurrent step must not request another approval.
            assert asyncio.run(dispatcher.step_dispatch()) is None
            if change == "priority":
                dispatcher.update_operation(second.id, {"priority": 20})
            elif change == "payload":
                dispatcher.update_operation(first.id, {"payload": {"changed": True}})
            elif change == "cancel":
                dispatcher.cancel_operation(first.id)
            else:
                dispatcher.request_stop()
        finally:
            release.set()
        assert pending.result(timeout=2) is None
    assert approvals == [first.id]
    assert dispatcher.current_operation is None
    assert not any(
        event.event_type is EventType.OPERATION_STARTED
        for event in dispatcher.get_history().events
    )
    if change != "stop":
        started = asyncio.run(dispatcher.step_dispatch())
        assert started.id == (first.id if change == "payload" else second.id)
        assert approvals[-1] == started.id


def test_active_operation_does_not_spin_runtime_and_completion_wakes_it(monkeypatch):
    async def scenario():
        dispatcher = OperationDispatcher("resource-a")
        first, second = add(dispatcher), add(dispatcher)
        await dispatcher.step_dispatch()
        iterations = 0
        original = dispatcher._runtime_service.step_dispatch

        async def count_step():
            nonlocal iterations
            iterations += 1
            return await original()

        monkeypatch.setattr(dispatcher._runtime_service, "step_dispatch", count_step)
        runtime = asyncio.create_task(dispatcher.run())
        try:
            await asyncio.sleep(0.03)
            assert iterations <= 3
            assert dispatcher.current_operation.id == first.id
            dispatcher.complete_operation(first.id)
            async with asyncio.timeout(2):
                while dispatcher.current_operation is None:
                    await asyncio.sleep(0.001)
            assert dispatcher.current_operation.id == second.id
        finally:
            dispatcher.request_stop()
            await asyncio.wait_for(runtime, timeout=2)

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["pause", "resume", "cancel"])
@pytest.mark.parametrize("running", [False, True])
def test_approval_callbacks_can_reenter_and_conflicting_result_is_preserved(
    action, running
):
    target = {
        "pause": EventType.OPERATION_PAUSE_REQUESTED,
        "resume": EventType.OPERATION_RESUME_REQUESTED,
        "cancel": EventType.OPERATION_CANCEL_REQUESTED,
    }[action]

    def approve(event):
        if event.event_type is target:
            dispatcher.complete_operation(event.operation_id)
        return True

    dispatcher = OperationDispatcher(
        "resource-a", on_request_callback=approve, request_event_timeout_seconds=0.5
    )
    operation = add(dispatcher)
    asyncio.run(dispatcher.step_dispatch())
    if action == "resume":
        assert dispatcher.pause_operation(operation.id)
    controller = OperationDispatcherRuntimeController(dispatcher)
    if running:
        controller.start()
    try:
        with pytest.raises(RuntimeError):
            getattr(dispatcher, f"{action}_operation")(operation.id)
        assert (
            dispatcher.get_operation(operation.id).outcome is ExecutionOutcome.SUCCESS
        )
        request = next(
            event
            for event in dispatcher.get_operation_events(operation.id)
            if event.event_type is target
        )
        assert request.meta_data["request_decision"]["accepted"] is True
        assert dispatcher.current_operation is None
    finally:
        if running:
            controller.stop()


@pytest.mark.parametrize("value", ["false", "true", 0, 1, [], {}, None])
def test_approval_requires_an_actual_boolean(value):
    dispatcher = OperationDispatcher(
        "resource-a", on_request_callback=lambda event: {"accepted": value}
    )
    operation = add(dispatcher)
    assert asyncio.run(dispatcher.step_dispatch()) is None
    assert dispatcher.get_operation(operation.id).state is ExecutionState.QUEUED
    denied = dispatcher.get_history().events[-1]
    assert denied.meta_data["reasoning"] == "invalid_callback_result"
    with pytest.raises(TypeError, match="boolean"):
        RequestDecision(accepted=value)


def test_timed_out_start_callback_does_not_block_event_loop_shutdown():
    release, finished = threading.Event(), threading.Event()

    def approve(event):
        try:
            release.wait(2)
            return True
        finally:
            finished.set()

    dispatcher = OperationDispatcher(
        "resource-a", on_request_callback=approve, request_event_timeout_seconds=0.03
    )
    add(dispatcher)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(asyncio.run, dispatcher.step_dispatch())
        try:
            assert pending.result(timeout=0.5) is None
            assert not finished.is_set()
        finally:
            release.set()
    assert finished.wait(2)
    assert dispatcher.current_operation is None


def test_runtime_pause_only_gates_dispatch():
    dispatcher = OperationDispatcher("resource-a")
    first, second = add(dispatcher), add(dispatcher)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.pause_dispatcher_runtime()
    assert dispatcher.current_operation.state is ExecutionState.RUNNING
    dispatcher.complete_operation(first.id)
    assert asyncio.run(dispatcher.step_dispatch()) is None
    dispatcher.resume_dispatcher_runtime()
    assert asyncio.run(dispatcher.step_dispatch()).id == second.id


@pytest.mark.parametrize(
    "field,value",
    [
        ("state", "COMPLETED"),
        ("outcome", "SUCCESS"),
        ("termination_reason", "USER_REQUEST"),
        ("retry_count", 3),
        ("start_time", None),
        ("finish_time", None),
    ],
)
def test_updates_cannot_bypass_lifecycle(field, value):
    dispatcher = OperationDispatcher("resource-a")
    operation = add(dispatcher)
    asyncio.run(dispatcher.step_dispatch())
    assert dispatcher.pause_operation(operation.id)
    before = dispatcher.get_operation(operation.id)
    with pytest.raises(ValueError, match="unsupported"):
        dispatcher.update_operation(operation.id, {field: value})
    assert dispatcher.get_operation(operation.id) == before
    with pytest.raises(ValueError):
        OperationDispatcher("resource-a", updatable_fields=[field])


@pytest.mark.parametrize("bucket", ["queued", "active", "completed"])
def test_duplicate_ids_rejected_in_every_bucket(bucket):
    dispatcher = OperationDispatcher("resource-a")
    operation = add(dispatcher)
    if bucket != "queued":
        asyncio.run(dispatcher.step_dispatch())
    if bucket == "completed":
        dispatcher.complete_operation(operation.id)
    for duplicate in (operation, operation.model_copy(deep=True)):
        with pytest.raises(ValueError, match="already exists"):
            dispatcher.add_operation(duplicate)
    if bucket == "completed":
        with pytest.raises(RuntimeError, match="completed"):
            dispatcher.update_operation(operation.id, {"payload": {"changed": True}})


def test_inputs_and_all_public_reads_are_detached_snapshots():
    notifications = []
    dispatcher = OperationDispatcher(
        "resource-a", on_notification_callback=notifications.append
    )
    metadata = {"nested": {"source": "caller"}}
    operation = Operation(resource_id="resource-a", payload={"nested": {"value": 1}})
    dispatcher.add_operation(operation, meta_data=metadata)
    operation.payload["nested"]["value"] = 99
    metadata["nested"]["source"] = "changed"
    notifications[0].meta_data["nested"]["source"] = "changed"
    snapshots = [
        dispatcher.get_operation(operation.id),
        dispatcher.get_schedule()[0],
        dispatcher.dispatch_queue.get(operation.id),
        dispatcher.get_history(resolve_operations=True).operations[0],
        dispatcher.update_operation(operation.id, {}),
    ]
    snapshots.append(asyncio.run(dispatcher.step_dispatch()))
    snapshots.extend(
        [dispatcher.current_operation, dispatcher.get_state().current_operation]
    )
    for snapshot in snapshots:
        snapshot.payload["nested"]["value"] = 100
        snapshot.state = ExecutionState.COMPLETED
    assert dispatcher.current_operation.payload == {"nested": {"value": 1}}
    assert dispatcher.current_operation.state is ExecutionState.RUNNING
    for event in (
        dispatcher.get_history().events[0],
        dispatcher.get_operation_events(operation.id)[0],
    ):
        event.meta_data["nested"]["source"] = "changed"
    assert dispatcher.get_history().events[0].meta_data["nested"]["source"] == "caller"
    completed = dispatcher.complete_operation(operation.id)
    completed.payload.clear()
    assert dispatcher.get_operation(operation.id).payload == {"nested": {"value": 1}}


def test_pruning_removes_only_old_completed_operations_and_preserves_events():
    dispatcher = OperationDispatcher("resource-a")
    completed = []
    for _ in range(3):
        operation = add(dispatcher)
        asyncio.run(dispatcher.step_dispatch())
        dispatcher.complete_operation(operation.id)
        completed.append(operation)
    active, queued = add(dispatcher), add(dispatcher)
    asyncio.run(dispatcher.step_dispatch())
    events = dispatcher.get_history().events
    assert dispatcher.prune_completed_operations(keep=1) == 2
    assert dispatcher.get_operation(completed[0].id) is None
    assert dispatcher.get_operation(completed[1].id) is None
    assert dispatcher.get_operation(completed[2].id).state is ExecutionState.COMPLETED
    assert dispatcher.current_operation.id == active.id
    assert dispatcher.get_schedule()[0].id == queued.id
    assert dispatcher.get_history().events == events
    assert dispatcher.prune_completed_operations() == 1
    for invalid in (-1, True, 1.5, "1"):
        with pytest.raises(ValueError):
            dispatcher.prune_completed_operations(keep=invalid)


def test_self_dependency_validation_checks_owner_not_dependency_record_id():
    provider_id, owner_id = uuid4(), uuid4()
    dependency = OperationDependency(
        id=provider_id,
        depends_on_operation_id=provider_id,
        dependency_type="FINISH_TO_START",
    )
    Operation(id=owner_id, resource_id="resource-a", dependencies=[dependency])
    with pytest.raises(ValueError, match="itself"):
        Operation(id=provider_id, resource_id="resource-a", dependencies=[dependency])


def test_notification_exceptions_are_logged_and_do_not_undo_mutation(caplog):
    def fail(event):
        raise ValueError("notification failed deliberately")

    with caplog.at_level(logging.ERROR):
        dispatcher = OperationDispatcher("resource-a", on_notification_callback=fail)
        operation = add(dispatcher)
    assert dispatcher.get_operation(operation.id) is not None
    assert "notification failed deliberately" in caplog.text

    async def scenario():
        async def fail_async(event):
            raise ValueError("async notification failed deliberately")

        dispatcher = OperationDispatcher(
            "resource-a", on_notification_callback=fail_async
        )
        add(dispatcher)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    with caplog.at_level(logging.ERROR):
        asyncio.run(scenario())
    assert "async notification failed deliberately" in caplog.text


def test_public_exports_are_resolvable():
    import operation_dispatcher

    namespace = {}
    exec("from operation_dispatcher import *", namespace)
    assert all(name in namespace for name in operation_dispatcher.__all__)
    assert namespace["RequestDecision"] is RequestDecision


def test_general_retry_settings_take_precedence_over_legacy_aliases():
    dispatcher = OperationDispatcher(
        "resource-a",
        on_request_callback=lambda event: False,
        start_request_max_retries=1,
        request_max_retries=2,
        start_request_retry_cooldown_seconds=3600,
        request_retry_cooldown_seconds=0,
    )
    add(dispatcher)
    assert asyncio.run(dispatcher.step_dispatch()) is None
    assert not dispatcher.is_paused
    assert asyncio.run(dispatcher.step_dispatch()) is None
    assert dispatcher.is_paused


def test_stop_from_async_start_callback_does_not_join_its_own_runtime_thread():
    stopped = threading.Event()

    async def approve(event):
        payload, status = controller.stop()
        assert status == 202
        assert payload["message"] == "operation dispatcher stop requested"
        stopped.set()
        return True

    dispatcher = OperationDispatcher("resource-a", on_request_callback=approve)
    controller = OperationDispatcherRuntimeController(
        dispatcher, startup_timeout_seconds=0.05
    )
    add(dispatcher)
    controller.start()
    try:
        assert stopped.wait(2)
        controller.runtime_thread.join(timeout=2)
        assert not controller.runtime_thread.is_alive()
        assert controller.runtime_last_error is None
        assert dispatcher.current_operation is None
    finally:
        controller.stop()
