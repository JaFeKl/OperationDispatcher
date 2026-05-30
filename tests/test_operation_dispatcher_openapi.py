import asyncio
from datetime import datetime, timezone
import time

import pytest

from operation_dispatcher import (
    ExecutionState,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    Operation,
    TerminationReason,
)
from operation_dispatcher.action_descriptions import OPENAPI_ACTION_DESCRIPTIONS


def _operation(
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
    assert payload[0]["operation"]["payload"]["task"] == "single"
    assert payload[0]["operation"]["state"] == ExecutionState.QUEUED.value
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
        item["operation"]["state"] == ExecutionState.QUEUED.value for item in payload
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

    first = _operation(resource_id="resource-a", priority=5)
    second = _operation(resource_id="resource-a", priority=3)
    dispatcher.add_operation(first)
    dispatcher.add_operation(second)
    asyncio.run(dispatcher.step_dispatch())

    all_payload, all_status = dispatcher_api.list_operations_response(state=None)
    running_payload, running_status = dispatcher_api.list_operations_response(
        state=ExecutionState.RUNNING.value,
    )

    assert all_status == 200
    assert running_status == 200
    assert len(all_payload) == 2
    assert len(running_payload) == 1
    assert running_payload[0]["operation"]["state"] == ExecutionState.RUNNING.value
    assert running_payload[0]["is_current_operation"] is True


def test_openapi_list_operations_excludes_terminal_states_and_history_contains_them() -> (
    None
):
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a", priority=1)
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(operation.id)

    listed_payload, listed_status = dispatcher_api.list_operations_response(state=None)
    history_payload, history_status = dispatcher_api.get_operations_history_response(
        limit=50
    )

    assert listed_status == 200
    assert all(item["operation"]["id"] != str(operation.id) for item in listed_payload)

    assert history_status == 200
    assert any(
        record["operation"]["id"] == str(operation.id)
        for record in history_payload["records"]
    )


def test_openapi_list_operations_rejects_terminal_state_filter() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.list_operations_response(
        state=ExecutionState.COMPLETED.value
    )

    assert status == 400
    assert payload["code"] == "invalid_state"
    assert payload["details"]["valid_states"] == [
        ExecutionState.PAUSED.value,
        ExecutionState.QUEUED.value,
        ExecutionState.RUNNING.value,
    ]


def test_openapi_get_operation_and_events() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)

    operation_payload, operation_status = dispatcher_api.get_operation_response(
        str(operation.id)
    )
    events_payload, events_status = dispatcher_api.get_operation_events_response(
        str(operation.id)
    )

    assert operation_status == 200
    assert operation_payload["operation"]["id"] == str(operation.id)
    assert operation_payload["operation"]["state"] == ExecutionState.QUEUED.value
    assert events_status == 200
    assert len(events_payload) >= 1


def test_openapi_get_current_operation_requires_running_operation() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    missing_payload, missing_status = dispatcher_api.get_current_operation_response()
    assert missing_status == 404
    assert missing_payload["code"] == "no_running_operation"

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    current_payload, current_status = dispatcher_api.get_current_operation_response()
    assert current_status == 200
    assert current_payload["operation"]["id"] == str(operation.id)
    assert current_payload["operation"]["state"] == ExecutionState.RUNNING.value
    assert current_payload["is_current_operation"] is True


def test_openapi_operation_pause_resume_cancel_commands() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    paused_payload, paused_status = dispatcher_api.pause_operation_response(
        str(operation.id)
    )
    assert paused_status == 200
    assert paused_payload["operation"]["state"] == ExecutionState.PAUSED.value

    resumed_payload, resumed_status = dispatcher_api.resume_operation_response(
        str(operation.id)
    )
    assert resumed_status == 200
    assert resumed_payload["operation"]["state"] == ExecutionState.RUNNING.value

    cancelled_payload, cancelled_status = dispatcher_api.cancel_operation_response(
        str(operation.id)
    )
    assert cancelled_status == 200
    assert cancelled_payload["operation"]["state"] == ExecutionState.CANCELLED.value


def test_openapi_operation_cancel_accepts_optional_meta_data_and_termination_reason() -> (
    None
):
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    cancelled_payload, cancelled_status = dispatcher_api.cancel_operation_response(
        str(operation.id),
        command_payload={
            "meta_data": {"source": "openapi", "ticket": "T-42"},
            "termination_reason": TerminationReason.USER_REQUEST.value,
        },
    )

    assert cancelled_status == 200
    assert cancelled_payload["operation"]["state"] == ExecutionState.CANCELLED.value
    assert (
        cancelled_payload["operation"]["termination_reason"]
        == TerminationReason.USER_REQUEST.value
    )

    operation_events_payload, operation_events_status = (
        dispatcher_api.get_operation_events_response(str(operation.id))
    )
    assert operation_events_status == 200

    cancel_requested_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_cancel_requested"
    ]
    cancelled_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_cancelled"
    ]
    assert len(cancel_requested_events) == 1
    assert len(cancelled_events) == 1
    assert cancel_requested_events[0]["meta_data"]["source"] == "openapi"
    assert cancelled_events[0]["meta_data"] == {
        "source": "openapi",
        "ticket": "T-42",
    }


def test_openapi_operation_pause_resume_accept_optional_meta_data() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    paused_payload, paused_status = dispatcher_api.pause_operation_response(
        str(operation.id),
        command_payload={"meta_data": {"source": "openapi", "action": "pause"}},
    )
    assert paused_status == 200
    assert paused_payload["operation"]["state"] == ExecutionState.PAUSED.value

    resumed_payload, resumed_status = dispatcher_api.resume_operation_response(
        str(operation.id),
        command_payload={"meta_data": {"source": "openapi", "action": "resume"}},
    )
    assert resumed_status == 200
    assert resumed_payload["operation"]["state"] == ExecutionState.RUNNING.value

    operation_events_payload, operation_events_status = (
        dispatcher_api.get_operation_events_response(str(operation.id))
    )
    assert operation_events_status == 200

    pause_requested_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_pause_requested"
    ]
    pause_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_paused"
    ]
    resume_requested_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_resume_requested"
    ]
    resume_events = [
        event
        for event in operation_events_payload
        if event["event_type"] == "operation_resumed"
    ]

    assert pause_requested_events[0]["meta_data"]["source"] == "openapi"
    assert pause_events[0]["meta_data"] == {"source": "openapi", "action": "pause"}
    assert resume_requested_events[0]["meta_data"]["source"] == "openapi"
    assert resume_events[0]["meta_data"] == {
        "source": "openapi",
        "action": "resume",
    }


def test_openapi_cancel_operation_rejects_invalid_optional_command_payload() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)

    payload, status = dispatcher_api.cancel_operation_response(
        str(operation.id),
        command_payload={"termination_reason": "NOT_A_REASON"},
    )

    assert status == 400
    assert payload["code"] == "invalid_termination_reason"


def test_openapi_pause_operation_rejects_invalid_meta_data() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

    payload, status = dispatcher_api.pause_operation_response(
        str(operation.id),
        command_payload={"meta_data": "invalid"},
    )

    assert status == 400
    assert payload["code"] == "invalid_meta_data"


def test_openapi_update_operation_updates_fields() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a", priority=1)
    dispatcher.add_operation(operation)

    updated_payload, updated_status = dispatcher_api.update_operation_response(
        str(operation.id),
        {
            "priority": 7,
            "payload": {"task": "patched", "retries": 1},
        },
    )

    assert updated_status == 200
    assert updated_payload["operation"]["id"] == str(operation.id)
    assert updated_payload["operation"]["priority"] == 7
    assert updated_payload["operation"]["payload"]["task"] == "patched"


def test_openapi_update_operation_rejects_invalid_update_payload_type() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)

    payload, status = dispatcher_api.update_operation_response(
        str(operation.id),
        ["not", "a", "dict"],
    )

    assert status == 400
    assert payload["code"] == "invalid_updates"


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

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    asyncio.run(dispatcher.step_dispatch())

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

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)

    deadline = time.time() + 0.5
    while dispatcher.current_operation is None and time.time() < deadline:
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
    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)
    assert (
        client.post(
            f"/operations/{operation.id}/update",
            json={"priority": 4},
        ).status_code
        == 200
    )
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

    operation = _operation(resource_id="resource-a")
    dispatcher.add_operation(operation)

    response = client.post(f"/operations/{operation.id}/cancel")

    assert response.status_code == 200
    response_payload = response.get_json()
    assert response_payload is not None
    assert response_payload["operation"]["id"] == str(operation.id)
    assert response_payload["operation"]["state"] == ExecutionState.CANCELLED.value


def test_openapi_definitions_include_operation_status_model() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)
    definitions = dispatcher_api.get_openapi_definitions()

    assert "OperationStatus" in definitions
    assert "Operation" in definitions
    assert "DispatchEvent" in definitions
    assert "AddOperationItem" in definitions
    assert "UpdateOperationItem" in definitions
    assert "LifecycleCommand" in definitions
    assert "CancelOperationCommand" in definitions
    assert definitions["DispatchEvent"]["properties"]["meta_data"]["default"] == {}
    assert definitions["LifecycleCommand"]["properties"]["meta_data"]["default"] == {}
    assert (
        definitions["CancelOperationCommand"]["properties"]["meta_data"]["default"]
        == {}
    )
    assert (
        definitions["OperationStatus"]["properties"]["operation"]["$ref"]
        == "#/definitions/Operation"
    )


def test_openapi_payload_schema_uses_default_payload_example() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    default_payload = {
        "name": "default_operation",
        "task": "default_task",
        "run_seconds": 5.0,
    }
    dispatcher_api = OperationDispatcherOpenAPI(
        dispatcher,
        default_operation_payload=default_payload,
    )
    definitions = dispatcher_api.get_openapi_definitions()

    add_payload_schema = definitions["AddOperationItem"]["properties"]["payload"]
    operation_payload_schema = definitions["Operation"]["properties"]["payload"]

    assert add_payload_schema["type"] == "object"
    assert add_payload_schema["example"] == default_payload
    assert operation_payload_schema["type"] == "object"
    assert operation_payload_schema["example"] == default_payload


def test_openapi_get_operation_reports_not_found() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

    payload, status = dispatcher_api.get_operation_response(
        "00000000-0000-0000-0000-000000000000"
    )

    assert status == 404
    assert payload["code"] == "operation_not_found"


def test_openapi_specs_use_centralized_action_descriptions() -> None:
    specs = {
        "list_operations": OperationDispatcherOpenAPI.list_operations_openapi_spec(),
        "get_operation": OperationDispatcherOpenAPI.get_operation_openapi_spec(),
        "get_current_operation": (
            OperationDispatcherOpenAPI.get_current_operation_openapi_spec()
        ),
        "get_operation_events": (
            OperationDispatcherOpenAPI.get_operation_events_openapi_spec()
        ),
        "get_operations_history": (
            OperationDispatcherOpenAPI.get_operations_history_openapi_spec()
        ),
        "add_operation": OperationDispatcherOpenAPI.add_operation_openapi_spec(),
        "cancel_operation": OperationDispatcherOpenAPI.cancel_operation_openapi_spec(),
        "update_operation": OperationDispatcherOpenAPI.update_operation_openapi_spec(),
        "pause_operation": OperationDispatcherOpenAPI.pause_operation_openapi_spec(),
        "resume_operation": OperationDispatcherOpenAPI.resume_operation_openapi_spec(),
        "get_dispatcher_state": (
            OperationDispatcherOpenAPI.get_operation_dispatcher_state_openapi_spec()
        ),
        "start_operation_dispatcher": (
            OperationDispatcherOpenAPI.start_operation_dispatcher_openapi_spec()
        ),
        "stop_operation_dispatcher": (
            OperationDispatcherOpenAPI.stop_operation_dispatcher_openapi_spec()
        ),
        "pause_operation_dispatcher": (
            OperationDispatcherOpenAPI.pause_operation_dispatcher_openapi_spec()
        ),
        "resume_operation_dispatcher": (
            OperationDispatcherOpenAPI.resume_operation_dispatcher_openapi_spec()
        ),
    }

    assert set(specs) == set(OPENAPI_ACTION_DESCRIPTIONS)

    for action_name, spec in specs.items():
        assert spec["description"] == OPENAPI_ACTION_DESCRIPTIONS[action_name].description
        assert spec["description"].strip() != ""
