import pytest
from pydantic import BaseModel
from typing import TypedDict

from operation_scheduler import Operation, Scheduler, SchedulerOpenAPI


class ExamplePayloadModel(BaseModel):
    task: str
    retries: int = 0


class ExamplePayloadTypedDict(TypedDict):
    task: str
    priority: int


def test_scheduler_openapi_get_schedule_response_returns_json_payload() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    payload, status_code = scheduler_api.get_schedule_response()

    assert status_code == 200
    assert len(payload) == 1
    assert payload[0]["id"] == str(operation.id)
    assert payload[0]["runtime_status"] == "pending"


def test_scheduler_openapi_get_schedule_spec_uses_scheduled_operation() -> None:
    spec = SchedulerOpenAPI.get_schedule_openapi_spec()

    assert spec["tags"] == ["Scheduler"]
    assert (
        spec["responses"][200]["schema"]["items"]["$ref"]
        == "#/definitions/ScheduledOperation"
    )


def test_scheduler_openapi_definitions_contains_required_models() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)
    definitions = scheduler_api.get_openapi_definitions()

    assert "ScheduledOperation" in definitions
    assert "AddOperationRequest" in definitions
    assert "TimeWindow" in definitions
    assert (
        definitions["ScheduledOperation"]["properties"]["payload"]["type"] == "object"
    )


def test_scheduler_openapi_add_operation_supports_time_window() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    added_payload, add_status = scheduler_api.add_operation_response(
        {
            "payload": {"source": "sensor"},
            "time_window": {
                "start": "2026-05-06T10:00:00Z",
                "end": "2026-05-06T10:30:00Z",
            },
        }
    )

    assert add_status == 201
    assert added_payload["time_window"]["start"] == "2026-05-06T10:00:00Z"


def test_scheduler_openapi_add_operation_returns_400_on_queue_type_mismatch() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    first_payload, first_status = scheduler_api.add_operation_response(
        {
            "payload": {"source": "sensor"},
        }
    )
    assert first_status == 201
    assert first_payload["time_window"] is None

    second_payload, second_status = scheduler_api.add_operation_response(
        {
            "payload": {"source": "sensor"},
            "time_window": {
                "start": "2026-05-06T10:00:00Z",
                "end": "2026-05-06T10:30:00Z",
            },
        }
    )

    assert second_status == 400
    assert "does not match queue type" in second_payload["error"]


def test_scheduler_openapi_register_get_schedule_endpoint_registers_route() -> None:
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    app = Flask(__name__)
    scheduler_api.register_get_schedule_endpoint(app)
    client = app.test_client()

    response = client.get("/scheduler/schedule")
    body = response.get_json()

    assert response.status_code == 200
    assert isinstance(body, list)
    assert body[0]["id"] == str(operation.id)


def test_scheduler_openapi_get_current_and_next_operation_responses() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)
    operation = Operation(name="sync", agent_id="agent-a")

    current_payload, current_status = scheduler_api.get_current_operation_response()
    assert current_status == 404
    assert current_payload["error"] == "no current operation"

    scheduler.add(operation)
    next_payload, next_status = scheduler_api.get_next_operation_response()
    assert next_status == 200
    assert next_payload["id"] == str(operation.id)

    scheduler.start_next()
    current_payload, current_status = scheduler_api.get_current_operation_response()
    assert current_status == 200
    assert current_payload["id"] == str(operation.id)


def test_scheduler_openapi_add_and_cancel_operation_responses() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    added_payload, add_status = scheduler_api.add_operation_response(
        {
            "priority": 3,
            "payload": {"source": "sensor"},
        }
    )
    assert add_status == 201
    assert added_payload["agent_id"] == "agent-a"
    assert added_payload["name"] == "operation"

    cancelled_payload, cancel_status = scheduler_api.cancel_operation_response(
        added_payload["id"]
    )
    assert cancel_status == 200
    assert cancelled_payload["result_status"] == "cancelled"


def test_scheduler_openapi_register_default_endpoints_exposes_required_routes() -> None:
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")
    operation = Operation(name="sync", agent_id="agent-a")
    scheduler.add(operation)

    app = Flask(__name__)
    scheduler_api.register_default_endpoints(app)
    client = app.test_client()

    assert client.get("/scheduler/schedule").status_code == 200
    assert client.get("/scheduler/history").status_code == 200
    assert client.get("/scheduler/current_operation").status_code == 404
    assert client.get("/scheduler/next_operation").status_code == 200
    assert client.get("/scheduler/state").status_code == 200

    add_response = client.post(
        "/scheduler/add_operation", json={"payload": {"task": "x"}}
    )
    assert add_response.status_code == 201
    added_id = add_response.get_json()["id"]

    cancel_response = client.post(
        "/scheduler/cancel_operation", json={"operation_id": added_id}
    )
    assert cancel_response.status_code == 200

    start_response = client.post("/scheduler/start")
    assert start_response.status_code == 200

    stop_response = client.post("/scheduler/stop")
    assert stop_response.status_code == 200


def test_scheduler_openapi_add_operation_requires_payload() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    response, status_code = scheduler_api.add_operation_response({"name": "sync"})

    assert status_code == 400
    assert response["error"] == "payload is required"


def test_scheduler_openapi_add_operation_requires_configured_agent_id() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)

    response, status_code = scheduler_api.add_operation_response(
        {"payload": {"task": "x"}}
    )

    assert status_code == 400
    assert "agent_id is not configured" in response["error"]


def test_scheduler_openapi_add_operation_schema_requires_payload_only() -> None:
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler)
    definitions = scheduler_api.get_openapi_definitions()

    assert definitions["AddOperationRequest"]["required"] == ["payload"]


def test_scheduler_openapi_validates_payload_with_pydantic_model() -> None:
    scheduler = Scheduler(payload_model=ExamplePayloadModel)
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    valid_payload, valid_status = scheduler_api.add_operation_response(
        {"payload": {"task": "collect_metrics", "retries": 2}}
    )
    assert valid_status == 201
    assert valid_payload["payload"]["task"] == "collect_metrics"

    invalid_payload, invalid_status = scheduler_api.add_operation_response(
        {"payload": {"retries": 2}}
    )
    assert invalid_status == 400
    assert "task" in invalid_payload["error"]


def test_scheduler_openapi_exposes_pydantic_payload_as_definition() -> None:
    scheduler = Scheduler(payload_model=ExamplePayloadModel)
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")
    definitions = scheduler_api.get_openapi_definitions()

    assert "ExamplePayloadModel" in definitions
    assert (
        definitions["AddOperationRequest"]["properties"]["payload"]["$ref"]
        == "#/definitions/ExamplePayloadModel"
    )


def test_scheduler_openapi_validates_payload_with_typed_dict_model() -> None:
    scheduler = Scheduler(payload_model=ExamplePayloadTypedDict)
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    valid_payload, valid_status = scheduler_api.add_operation_response(
        {"payload": {"task": "inspect", "priority": 5}}
    )
    assert valid_status == 201
    assert valid_payload["payload"]["task"] == "inspect"

    invalid_payload, invalid_status = scheduler_api.add_operation_response(
        {"payload": {"task": "inspect"}}
    )
    assert invalid_status == 400
    assert "priority" in invalid_payload["error"]


def test_scheduler_openapi_exposes_typed_dict_payload_as_definition() -> None:
    scheduler = Scheduler(payload_model=ExamplePayloadTypedDict)
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")
    definitions = scheduler_api.get_openapi_definitions()

    assert "ExamplePayloadTypedDict" in definitions
    assert (
        definitions["AddOperationRequest"]["properties"]["payload"]["$ref"]
        == "#/definitions/ExamplePayloadTypedDict"
    )


def test_scheduler_openapi_runtime_start_stop_and_state() -> None:
    scheduler = Scheduler(poll_interval_seconds=0.01)
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    start_payload, start_status = scheduler_api.start_scheduler_response()
    assert start_status == 200
    assert "message" in start_payload
    assert "state" in start_payload
    assert start_payload["state"]["runtime_thread_alive"] is True

    state_payload, state_status = scheduler_api.get_scheduler_state_response()
    assert state_status == 200
    assert "is_running" in state_payload
    assert "queue_size" in state_payload
    assert "runtime_thread_alive" in state_payload

    stop_payload, stop_status = scheduler_api.stop_scheduler_response()
    assert stop_status == 200
    assert "message" in stop_payload
    assert "state" in stop_payload
    assert stop_payload["state"]["is_running"] is False


def test_scheduler_openapi_history_response_uses_limit_and_returns_newest_first() -> (
    None
):
    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    first_payload, _ = scheduler_api.add_operation_response({"payload": {"step": 1}})
    second_payload, _ = scheduler_api.add_operation_response({"payload": {"step": 2}})

    first_id = first_payload["id"]
    second_id = second_payload["id"]

    pulled_first = scheduler.start_next()
    assert pulled_first is not None
    scheduler.complete_current()

    pulled_second = scheduler.start_next()
    assert pulled_second is not None
    scheduler.complete_current()

    history_payload, status_code = scheduler_api.get_schedule_history_response(limit=1)

    assert status_code == 200
    assert history_payload["limit"] == 1
    assert history_payload["count"] == 1
    assert history_payload["operations"][0]["id"] in {first_id, second_id}


def test_scheduler_openapi_history_endpoint_rejects_invalid_limit_query() -> None:
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask

    scheduler = Scheduler()
    scheduler_api = SchedulerOpenAPI(scheduler, agent_id="agent-a")

    app = Flask(__name__)
    scheduler_api.register_get_schedule_history_endpoint(app)
    client = app.test_client()

    response = client.get("/scheduler/history?limit=abc")

    assert response.status_code == 400
    assert response.get_json()["error"] == "limit must be an integer"
