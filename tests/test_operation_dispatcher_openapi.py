from datetime import datetime, timedelta, timezone

import pytest
from pydantic import BaseModel

from operation_dispatcher import (
    Operation,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
)


class ExampleOperationModel(Operation):
    task: str
    retries: int = 0


def _scheduled_operation(
    *,
    resource_id: str = "resource-a",
    priority: int = 0,
    release_date: datetime | None = None,
) -> ScheduledOperation:
    return ScheduledOperation(
        operation=ExampleOperationModel(task="sync"),
        resource_id=resource_id,
        priority=priority,
        release_date=release_date,
    )


def test_dispatcher_openapi_get_dispatch_queue_response_returns_json_payload() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)

    payload, status_code = dispatcher_api.get_dispatch_queue_response()

    assert status_code == 200
    assert len(payload) == 1
    assert payload[0]["operation"]["id"] == str(scheduled_operation.operation.id)


def test_dispatcher_openapi_get_current_and_next_operation_responses() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    scheduled_operation = _scheduled_operation(resource_id="resource-a")

    current_payload, current_status = dispatcher_api.get_current_operation_response()
    assert current_status == 404
    assert current_payload["error"] == "no current operation"

    dispatcher.add(scheduled_operation)
    next_payload, next_status = dispatcher_api.get_next_operation_response()
    assert next_status == 200
    assert next_payload["operation"]["id"] == str(scheduled_operation.operation.id)

    dispatcher._start_next()
    current_payload, current_status = dispatcher_api.get_current_operation_response()
    assert current_status == 200
    assert current_payload["operation"]["id"] == str(scheduled_operation.operation.id)


def test_dispatcher_openapi_add_and_cancel_operation_responses() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    added_payload, add_status = dispatcher_api.add_operation_response(
        [
            {
                "priority": 3,
                "operation": {"task": "collect", "retries": 1},
            }
        ]
    )
    assert add_status == 201
    assert len(added_payload) == 1
    assert added_payload[0]["resource_id"] == "resource-a"
    assert "id" in added_payload[0]["operation"]

    cancelled_payload, cancel_status = dispatcher_api.cancel_operation_response(
        added_payload[0]["operation"]["id"]
    )
    assert cancel_status == 200
    assert cancelled_payload["operation"]["id"] == added_payload[0]["operation"]["id"]


def test_dispatcher_openapi_add_operation_response_supports_batch_payload() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    added_payload, add_status = dispatcher_api.add_operation_response(
        [
            {"operation": {"task": "collect", "retries": 1}, "priority": 3},
            {"operation": {"task": "inspect", "retries": 0}, "priority": 2},
        ]
    )

    assert add_status == 201
    assert len(added_payload) == 2
    assert len(dispatcher.dispatch_queue) == 2


def test_dispatcher_openapi_add_operation_requires_operation_field() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    response, status_code = dispatcher_api.add_operation_response({"priority": 1})

    assert status_code == 400
    assert response["code"] == "invalid_operations"


def test_dispatcher_openapi_validates_operation_model_payload() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    valid_payload, valid_status = dispatcher_api.add_operation_response(
        [{"operation": {"task": "inspect", "retries": 2}}]
    )
    assert valid_status == 201
    assert "id" in valid_payload[0]["operation"]

    invalid_payload, invalid_status = dispatcher_api.add_operation_response(
        [{"operation": {"retries": 2}}]
    )
    assert invalid_status == 400
    assert invalid_payload["code"] == "invalid_operation"


def test_dispatcher_openapi_runtime_start_stop_and_state() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
        poll_interval_seconds=0.01,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    start_payload, start_status = dispatcher_api.start_operation_dispatcher_response()
    assert start_status == 202
    assert "state" in start_payload
    assert start_payload["state"]["runtime_thread_alive"] is True

    state_payload, state_status = (
        dispatcher_api.get_operation_dispatcher_state_response()
    )
    assert state_status == 200
    assert "is_running" in state_payload
    assert "queue_size" in state_payload

    stop_payload, stop_status = dispatcher_api.stop_operation_dispatcher_response()
    assert stop_status == 202
    assert stop_payload["state"]["is_running"] is False


def test_dispatcher_openapi_runtime_stop_returns_409_for_invalid_state() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
        poll_interval_seconds=0.01,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    stop_payload, stop_status = dispatcher_api.stop_operation_dispatcher_response()
    assert stop_status == 409
    assert "state" in stop_payload


def test_dispatcher_openapi_current_operation_actions() -> None:
    def deny_pause_and_resume(event) -> bool | None:
        if event.event_type.value in {
            "operation_pause_requested",
            "operation_resume_requested",
        }:
            return False
        return None

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
        on_request_callback=deny_pause_and_resume,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    missing_cancel_payload, missing_cancel_status = (
        dispatcher_api.cancel_current_operation_response()
    )
    assert missing_cancel_status == 404
    assert missing_cancel_payload["code"] == "no_current_operation"

    scheduled_operation = _scheduled_operation(resource_id="resource-a")
    dispatcher.add(scheduled_operation)
    dispatcher._start_next()

    denied_pause_payload, denied_pause_status = (
        dispatcher_api.pause_current_operation_response()
    )
    assert denied_pause_status == 409
    assert denied_pause_payload["code"] == "current_operation_pause_denied"

    denied_resume_payload, denied_resume_status = (
        dispatcher_api.resume_current_operation_response()
    )
    assert denied_resume_status == 409
    assert denied_resume_payload["code"] == "current_operation_not_paused"


def test_dispatcher_openapi_register_default_endpoints_exposes_required_routes() -> (
    None
):
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    app = Flask(__name__)
    dispatcher_api.register_default_endpoints(app)
    client = app.test_client()

    assert client.get("/operation_dispatcher/queue").status_code == 200
    assert client.get("/operation_dispatcher/history").status_code == 200
    assert client.get("/operation_dispatcher/current_operation").status_code == 404
    assert client.get("/operation_dispatcher/next_operation").status_code == 404
    assert client.get("/operation_dispatcher/state").status_code == 200
    assert client.post("/operation_dispatcher/add", json={}).status_code == 400
    assert (
        client.post("/operation_dispatcher/current_operation/cancel").status_code == 404
    )
    assert (
        client.post("/operation_dispatcher/current_operation/pause").status_code == 404
    )
    assert (
        client.post("/operation_dispatcher/current_operation/resume").status_code == 404
    )


def test_dispatcher_openapi_definitions_include_operation_model() -> None:
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    definitions = dispatcher_api.get_openapi_definitions()

    assert "ScheduledOperation" in definitions
    assert "AddOperationRequest" in definitions
    assert "AddOperationItem" in definitions
    assert "ExampleOperationModel" in definitions
    assert definitions["AddOperationRequest"]["type"] == "array"
    assert (
        definitions["AddOperationItem"]["properties"]["operation"]["$ref"]
        == "#/definitions/ExampleOperationModel"
    )


def test_dispatcher_openapi_history_response_uses_limit_and_returns_newest_first() -> (
    None
):
    dispatcher = OperationDispatcher(
        resource_id="resource-a",
        operation_model=ExampleOperationModel,
    )
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    first_payload, _ = dispatcher_api.add_operation_response(
        [{"operation": {"task": "first"}}]
    )
    second_payload, _ = dispatcher_api.add_operation_response(
        [{"operation": {"task": "second"}}]
    )

    first_id = first_payload[0]["operation"]["id"]
    second_id = second_payload[0]["operation"]["id"]

    pulled_first = dispatcher._start_next()
    assert pulled_first is not None
    dispatcher.complete_current()

    pulled_second = dispatcher._start_next()
    assert pulled_second is not None
    dispatcher.complete_current()

    history_payload, status_code = dispatcher_api.get_dispatch_history_response(limit=1)

    assert status_code == 200
    assert history_payload["limit"] == 1
    assert history_payload["count"] == 1
    history_entry = history_payload["entries"][0]
    assert history_entry["scheduled_operation"]["operation"]["id"] in {
        first_id,
        second_id,
    }
    assert "execution" in history_entry
    assert "events" in history_entry


def test_dispatcher_openapi_specs_include_required_input_parameters() -> None:
    history_spec = OperationDispatcherOpenAPI.get_dispatch_history_openapi_spec()
    assert "parameters" in history_spec
    assert history_spec["parameters"][0]["name"] == "limit"
    assert history_spec["parameters"][0]["in"] == "query"

    add_spec = OperationDispatcherOpenAPI.add_operation_openapi_spec()
    assert "parameters" in add_spec
    assert add_spec["parameters"][0]["in"] == "body"
    assert (
        add_spec["parameters"][0]["schema"]["$ref"]
        == "#/definitions/AddOperationRequest"
    )

    cancel_spec = OperationDispatcherOpenAPI.cancel_operation_openapi_spec()
    assert "parameters" in cancel_spec
    assert cancel_spec["parameters"][0]["in"] == "body"
    assert (
        cancel_spec["parameters"][0]["schema"]["$ref"]
        == "#/definitions/CancelOperationRequest"
    )
