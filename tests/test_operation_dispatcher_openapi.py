from datetime import datetime, timezone
import time

import pytest

from operation_dispatcher import (
    ExecutionState,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    Operation,
)


def _scheduled_operation(
    *,
    resource_id: str = "resource-a",
    priority: int = 0,
) -> Operation:
    return Operation(
        payload={"task": "sync", "retries": 0},
        resource_id=resource_id,
        priority=priority,
    )


def test_openapi_add_single_operation_returns_operation_status() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.add_operation_response(
        [
            {
                "payload": {"task": "single"},
                "priority": 2,
            }
        ]
    )

    assert status == 201
    assert len(payload) == 1
    assert payload[0]["scheduled_operation"]["payload"]["task"] == "single"
    assert payload[0]["execution"]["state"] == ExecutionState.QUEUED.value
    assert payload[0]["is_current_operation"] is False


def test_openapi_add_operations_returns_operation_status_list() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.add_operation_response(
        [
            {"payload": {"task": "first"}, "priority": 3},
            {"payload": {"task": "second"}, "priority": 1},
        ]
    )

    assert status == 201
    assert len(payload) == 2
    assert all(
        item["execution"]["state"] == ExecutionState.QUEUED.value for item in payload
    )


def test_openapi_add_operation_rejects_resource_id_in_payload() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.add_operation_response(
        [
            {
                "payload": {"task": "single"},
                "resource_id": "resource-b",
            }
        ]
    )

    assert status == 400
    assert payload["code"] == "resource_id_not_allowed"


def test_openapi_list_operations_and_filter_by_state() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    first = _scheduled_operation(resource_id="resource-a", priority=5)
    second = _scheduled_operation(resource_id="resource-a", priority=3)
    dispatcher.add(first)
    dispatcher.add(second)
    dispatcher._start_next()

    all_payload, all_status = dispatcher_api.list_operations_response(state=None)
    running_payload, running_status = dispatcher_api.list_operations_response(
        state=ExecutionState.RUNNING.value,
    )

    assert all_status == 200
    assert running_status == 200
    assert len(all_payload) == 2
    assert len(running_payload) == 1
    assert running_payload[0]["execution"]["state"] == ExecutionState.RUNNING.value
    assert running_payload[0]["is_current_operation"] is True


def test_openapi_get_operation_and_events() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)

    operation_payload, operation_status = dispatcher_api.get_operation_response(
        str(scheduled_operation.id)
    )
    events_payload, events_status = dispatcher_api.get_operation_events_response(
        str(scheduled_operation.id)
    )

    assert operation_status == 200
    assert operation_payload["scheduled_operation"]["id"] == str(scheduled_operation.id)
    assert operation_payload["execution"]["state"] == ExecutionState.QUEUED.value
    assert events_status == 200
    assert len(events_payload) >= 1


def test_openapi_get_current_operation_requires_running_operation() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    missing_payload, missing_status = dispatcher_api.get_current_operation_response()
    assert missing_status == 404
    assert missing_payload["code"] == "no_running_operation"

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    current_payload, current_status = dispatcher_api.get_current_operation_response()
    assert current_status == 200
    assert current_payload["scheduled_operation"]["id"] == str(scheduled_operation.id)
    assert current_payload["execution"]["state"] == ExecutionState.RUNNING.value
    assert current_payload["is_current_operation"] is True


def test_openapi_operation_pause_resume_cancel_commands() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    paused_payload, paused_status = dispatcher_api.pause_operation_response(
        str(scheduled_operation.id)
    )
    assert paused_status == 200
    assert paused_payload["execution"]["state"] == ExecutionState.PAUSED.value

    resumed_payload, resumed_status = dispatcher_api.resume_operation_response(
        str(scheduled_operation.id)
    )
    assert resumed_status == 200
    assert resumed_payload["execution"]["state"] == ExecutionState.RUNNING.value

    cancelled_payload, cancelled_status = dispatcher_api.cancel_operation_response(
        str(scheduled_operation.id)
    )
    assert cancelled_status == 200
    assert cancelled_payload["execution"]["state"] == ExecutionState.CANCELLED.value


def test_openapi_dispatcher_runtime_endpoints() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        poll_interval_seconds=0.01,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    start_payload, start_status = dispatcher_api.start_operation_dispatcher_response()
    assert start_status == 202
    assert "state" in start_payload

    state_payload, state_status = (
        dispatcher_api.get_operation_dispatcher_state_response()
    )
    assert state_status == 200
    assert "is_running" in state_payload

    stop_payload, stop_status = dispatcher_api.stop_operation_dispatcher_response()
    assert stop_status == 202
    assert stop_payload["state"]["is_running"] is False


def test_openapi_dispatcher_pause_resume_do_not_trigger_operation_requests() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    pause_payload, pause_status = dispatcher_api.pause_operation_dispatcher_response()
    assert pause_status == 200
    assert pause_payload["state"]["is_paused"] is True

    resume_payload, resume_status = (
        dispatcher_api.resume_operation_dispatcher_response()
    )
    assert resume_status == 200
    assert resume_payload["state"]["is_paused"] is False

    event_types = [event.event_type.value for event in dispatcher.get_event_history()]
    assert "operation_dispatcher_paused" in event_types
    assert "operation_dispatcher_resumed" in event_types
    assert "operation_pause_requested" not in event_types
    assert "operation_resume_requested" not in event_types


def test_openapi_dispatcher_stop_does_not_trigger_operation_cancel_request() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    start_payload, start_status = dispatcher_api.start_operation_dispatcher_response()
    assert start_status == 202
    assert "state" in start_payload

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)

    deadline = time.time() + 0.5
    while dispatcher.current_scheduled_operation is None and time.time() < deadline:
        time.sleep(0.01)

    stop_payload, stop_status = dispatcher_api.stop_operation_dispatcher_response()
    assert stop_status == 202
    assert stop_payload["state"]["is_running"] is False

    event_types = [event.event_type.value for event in dispatcher.get_event_history()]
    assert "operation_dispatcher_stopped" in event_types
    assert "operation_cancel_requested" not in event_types


def test_openapi_register_default_endpoints_exposes_new_contract_routes() -> None:
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    app = Flask(__name__)
    dispatcher_api.register_default_endpoints(app)
    client = app.test_client()

    now = datetime.now(timezone.utc)
    _ = now

    assert client.get("/operations").status_code == 200
    assert client.get("/operations/current").status_code == 404
    assert client.get("/operations/history").status_code == 200
    assert client.post("/operations/add", json={}).status_code == 400
    assert client.post("/operations", json=[]).status_code == 405
    assert client.post("/operations:batch", json=[]).status_code == 404
    assert client.get("/dispatcher").status_code == 200
    assert client.post("/dispatcher/start").status_code in {202, 409}
    assert client.post("/dispatcher/stop").status_code in {202, 409}

    # Old contract routes should no longer be present.
    assert client.get("/operation_dispatcher/queue").status_code == 404


def test_openapi_cancel_endpoint_accepts_path_operation_id_kwarg() -> None:
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    app = Flask(__name__)
    dispatcher_api.register_default_endpoints(app)
    client = app.test_client()

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)

    response = client.post(f"/operations/{scheduled_operation.id}/cancel")

    assert response.status_code == 200
    response_payload = response.get_json()
    assert response_payload is not None
    assert response_payload["scheduled_operation"]["id"] == str(scheduled_operation.id)
    assert response_payload["execution"]["state"] == ExecutionState.CANCELLED.value


def test_openapi_definitions_include_operation_status_model() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    definitions = dispatcher_api.get_openapi_definitions()

    assert "OperationStatus" in definitions
    assert "Operation" in definitions
    assert "OperationExecution" in definitions
    assert "AddOperationItem" in definitions
    assert (
        definitions["OperationStatus"]["properties"]["scheduled_operation"]["$ref"]
        == "#/definitions/Operation"
    )


def test_openapi_get_operation_reports_not_found() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.get_operation_response(
        "00000000-0000-0000-0000-000000000000"
    )

    assert status == 404
    assert payload["code"] == "operation_not_found"
