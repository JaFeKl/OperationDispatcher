import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

pytest.importorskip("flask")
pytest.importorskip("flasgger")

from operation_dispatcher import (
    EventType,
    ExecutionOutcome,
    ExecutionState,
    Operation,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    OperationDispatcherRuntimeController,
    TerminationReason,
)


def active_dispatcher(callback=lambda event: True, **kwargs):
    dispatcher = OperationDispatcher(
        resource_id="resource-a", on_request_callback=callback, **kwargs
    )
    operation = Operation(resource_id="resource-a", payload={})
    dispatcher.add_operation(operation)
    assert asyncio.run(dispatcher.step_dispatch()).id == operation.id
    return dispatcher, operation


def events(dispatcher, operation):
    return [
        event
        for event in dispatcher.get_history(limit=1000).events
        if event.operation_id == operation.id
    ]


@pytest.mark.parametrize("paused", [False, True])
@pytest.mark.parametrize(
    "outcome", [ExecutionOutcome.SUCCESS, ExecutionOutcome.FAILURE]
)
def test_approved_completion_records_decision_and_releases_slot(outcome, paused):
    requests, notifications = [], []

    async def approve(event):
        requests.append(event)
        return {"accepted": True, "reasoning": "operator_allowed"}

    dispatcher, operation = active_dispatcher(
        approve, on_notification_callback=notifications.append
    )
    if paused:
        assert dispatcher.pause_operation(operation.id)
    notifications.clear()
    result = dispatcher.request_operation_completion(
        operation.id, outcome, meta_data={"source": "operator", "outcome": "spoofed"}
    )
    assert result.id == operation.id
    assert result.state is ExecutionState.COMPLETED
    assert result.outcome is outcome
    assert result.finish_time is not None
    assert result.termination_reason is (
        TerminationReason.NONE
        if outcome is ExecutionOutcome.SUCCESS
        else TerminationReason.INTERNAL_ERROR
    )
    assert dispatcher.current_operation is None
    assert requests[-1].meta_data["outcome"] == outcome.value
    request, terminal = events(dispatcher, operation)[-2:]
    assert request.event_type is EventType.OPERATION_COMPLETE_REQUESTED
    assert request.meta_data["request_decision"]["reasoning"] == "operator_allowed"
    assert terminal.event_type is (
        EventType.OPERATION_COMPLETED
        if outcome is ExecutionOutcome.SUCCESS
        else EventType.OPERATION_FAILED
    )
    assert terminal.meta_data["source"] == "operator"
    assert [event.event_type for event in notifications] == [terminal.event_type]
    next_operation = Operation(resource_id="resource-a", payload={})
    dispatcher.add_operation(next_operation)
    assert asyncio.run(dispatcher.step_dispatch()).id == next_operation.id


@pytest.mark.parametrize(
    "behavior", ["missing", "false", "invalid", "exception", "timeout", "async_timeout"]
)
def test_completion_denial_keeps_operation_and_does_not_pause(behavior):
    release = threading.Event()
    finished = threading.Event()

    async def delayed():
        await asyncio.sleep(1)
        return True

    def callback(event):
        if event.event_type is not EventType.OPERATION_COMPLETE_REQUESTED:
            return True
        if behavior == "exception":
            raise ValueError("cannot approve")
        if behavior == "timeout":
            try:
                release.wait(2)
                return True
            finally:
                finished.set()
        if behavior == "async_timeout":
            return delayed()
        if behavior == "invalid":
            return None
        return {
            "accepted": False,
            "reasoning": "busy",
            "data": {"resource": "occupied"},
        }

    dispatcher, operation = active_dispatcher(
        None if behavior == "missing" else callback,
        start_request_max_retries=1,
        request_event_timeout_seconds=0.05,
    )
    before = dispatcher.get_operation(operation.id).model_dump()
    try:
        for _ in range(2):
            assert (
                dispatcher.request_operation_completion(
                    operation.id, ExecutionOutcome.SUCCESS
                )
                is None
            )
        assert dispatcher.get_operation(operation.id).model_dump() == before
        assert not dispatcher.is_paused
        request, denied = events(dispatcher, operation)[-2:]
        assert request.meta_data["request_decision"]["accepted"] is False
        assert denied.event_type is EventType.OPERATION_COMPLETE_DENIED
        assert "retry_count" not in denied.meta_data
        if behavior == "false":
            assert denied.meta_data["reasoning"] == "busy"
            assert denied.meta_data["decision_data"] == {"resource": "occupied"}
    finally:
        release.set()
        if behavior == "timeout":
            assert finished.wait(2)
    assert dispatcher.get_operation(operation.id).state is ExecutionState.RUNNING
    # Existing local result reporting still bypasses approval.
    dispatcher.complete_operation(operation.id)
    assert dispatcher.get_operation(operation.id).outcome is ExecutionOutcome.SUCCESS


@pytest.mark.parametrize("runtime_running", [False, True])
def test_competing_completion_requests_finalize_once(runtime_running):
    barrier = threading.Barrier(2)

    def callback(event):
        if event.event_type is EventType.OPERATION_COMPLETE_REQUESTED:
            barrier.wait(timeout=2)
        return True

    dispatcher, operation = active_dispatcher(callback)
    controller = OperationDispatcherRuntimeController(dispatcher)
    if runtime_running:
        controller.start()
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            api = OperationDispatcherOpenAPI(dispatcher)
            futures = [
                executor.submit(
                    api.complete_operation_response,
                    str(operation.id),
                    {"outcome": outcome},
                )
                for outcome in ("SUCCESS", "FAILURE")
            ]
            results = [future.result(timeout=5) for future in futures]
        assert sorted(status for _, status in results) == [200, 409]
        terminal = [
            event
            for event in events(dispatcher, operation)
            if event.event_type
            in (EventType.OPERATION_COMPLETED, EventType.OPERATION_FAILED)
        ]
        assert len(terminal) == 1
    finally:
        if runtime_running:
            controller.stop()


@pytest.mark.parametrize("runtime_running", [False, True])
def test_local_completion_during_approval_wins_without_deadlock(runtime_running):
    def callback(event):
        if event.event_type is EventType.OPERATION_COMPLETE_REQUESTED:
            dispatcher.fail_operation(event.operation_id)
        return True

    dispatcher, operation = active_dispatcher(callback)
    controller = OperationDispatcherRuntimeController(dispatcher)
    if runtime_running:
        controller.start()
    try:
        payload, status = OperationDispatcherOpenAPI(
            dispatcher
        ).complete_operation_response(str(operation.id), {"outcome": "SUCCESS"})
        assert status == 409
        assert payload["code"] == "operation_completion_conflict"
        assert (
            dispatcher.get_operation(operation.id).outcome is ExecutionOutcome.FAILURE
        )
        assert (
            len(
                [
                    event
                    for event in events(dispatcher, operation)
                    if event.event_type is EventType.OPERATION_FAILED
                ]
            )
            == 1
        )
    finally:
        if runtime_running:
            controller.stop()


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"outcome": "NONE"},
        {"outcome": "CANCELLED"},
        {"outcome": "success"},
        {"outcome": "SUCCESS", "meta_data": []},
        {"outcome": "SUCCESS", "termination_reason": "TIMEOUT"},
        {"outcome": "FAILURE", "termination_reason": "unknown"},
    ],
)
def test_api_rejects_invalid_completion_commands_without_callback(body):
    calls = []
    dispatcher, operation = active_dispatcher(lambda event: calls.append(event) or True)
    calls.clear()
    _, status = OperationDispatcherOpenAPI(dispatcher).complete_operation_response(
        str(operation.id), body
    )
    assert status == 400
    assert calls == []
    assert dispatcher.get_operation(operation.id).state is ExecutionState.RUNNING


def test_api_completion_ids_states_and_duplicate_requests():
    dispatcher, operation = active_dispatcher()
    api = OperationDispatcherOpenAPI(dispatcher)
    body = {"outcome": "SUCCESS"}
    assert api.complete_operation_response("invalid", body)[1] == 400
    assert api.complete_operation_response(str(uuid4()), body)[1] == 404
    queued = Operation(resource_id="resource-a", payload={})
    dispatcher.add_operation(queued)
    assert api.complete_operation_response(str(queued.id), body)[1] == 409
    assert api.complete_operation_response(str(operation.id), body)[1] == 200
    assert api.complete_operation_response(str(operation.id), body)[1] == 409


@pytest.mark.parametrize("outcome", ["SUCCESS", "FAILURE"])
def test_flask_completion_endpoint_and_openapi(outcome):
    from flasgger import Swagger
    from flask import Flask

    dispatcher, operation = active_dispatcher()
    api = OperationDispatcherOpenAPI(dispatcher)
    app = Flask(__name__)
    api.register_default_endpoints(app)
    Swagger(app, template={"definitions": api.get_openapi_definitions()})
    client = app.test_client()
    body = {"outcome": outcome, "meta_data": {"source": "remote"}}
    if outcome == "FAILURE":
        body["termination_reason"] = "EXTERNAL_ERROR"
    response = client.post(f"/operations/{operation.id}/complete", json=body)
    assert response.status_code == 200
    assert response.json["operation"]["outcome"] == outcome
    assert response.json["is_current_operation"] is False
    if outcome == "FAILURE":
        assert response.json["operation"]["termination_reason"] == "EXTERNAL_ERROR"
    spec = client.get("/apispec_1.json").json
    assert "/operations/{operation_id}/complete" in spec["paths"]
    command = spec["definitions"]["CompleteOperationCommand"]
    assert command["required"] == ["outcome"]
    assert command["properties"]["outcome"]["enum"] == ["SUCCESS", "FAILURE"]
    assert (
        client.post(
            f"/operations/{operation.id}/complete",
            data="{",
            content_type="application/json",
        ).status_code
        == 400
    )


def test_completion_validation_also_applies_to_python_facade():
    dispatcher, operation = active_dispatcher()
    with pytest.raises(ValueError):
        dispatcher.request_operation_completion(
            operation.id, ExecutionOutcome.CANCELLED
        )
    with pytest.raises(ValueError):
        dispatcher.request_operation_completion(
            operation.id, ExecutionOutcome.SUCCESS, TerminationReason.TIMEOUT
        )


def test_callback_cannot_change_completion_command_or_forge_decision():
    def callback(event):
        if event.event_type is EventType.OPERATION_COMPLETE_REQUESTED:
            event.meta_data["outcome"] = "FAILURE"
            event.meta_data["details"]["source"] = "changed"
        return True

    dispatcher, operation = active_dispatcher(callback)
    metadata = {
        "request_decision": {"accepted": False},
        "details": {"source": "operator"},
    }
    dispatcher.request_operation_completion(
        operation.id, ExecutionOutcome.SUCCESS, meta_data=metadata
    )
    request, terminal = events(dispatcher, operation)[-2:]
    assert dispatcher.get_operation(operation.id).outcome is ExecutionOutcome.SUCCESS
    assert request.meta_data["outcome"] == "SUCCESS"
    assert request.meta_data["request_decision"]["accepted"] is True
    assert terminal.meta_data["details"] == {"source": "operator"}
    assert "request_decision" not in terminal.meta_data
    assert metadata["details"]["source"] == "operator"


def test_api_denial_does_not_complete_operation():
    dispatcher, operation = active_dispatcher(None)
    api = OperationDispatcherOpenAPI(dispatcher)
    payload, status = api.complete_operation_response(
        str(operation.id), {"outcome": "SUCCESS"}
    )
    assert status == 409
    assert payload["code"] == "operation_completion_denied"
    assert dispatcher.get_operation(operation.id).state is ExecutionState.RUNNING


def test_runtime_loop_propagates_completion_conflict_without_deadlock():
    # Run on a daemon thread so a regression cannot hang the test process.
    done = threading.Event()
    errors = []

    async def scenario():
        dispatcher, operation = OperationDispatcher(
            resource_id="resource-a"
        ), Operation(resource_id="resource-a", payload={})
        dispatcher.add_operation(operation)
        await dispatcher.step_dispatch()
        runtime = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0)
        try:
            dispatcher.complete_operation(operation.id)
            with pytest.raises(RuntimeError):
                dispatcher.request_operation_completion(
                    operation.id, ExecutionOutcome.SUCCESS
                )
        finally:
            dispatcher.request_stop()
            await runtime

    def run():
        try:
            asyncio.run(scenario())
        except (
            BaseException
        ) as error:  # noqa: BLE001 - report thread failures to pytest
            errors.append(error)
        finally:
            done.set()

    threading.Thread(target=run, daemon=True).start()
    assert done.wait(3), "completion conflict blocked the runtime loop"
    assert errors == []
