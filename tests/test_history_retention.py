import asyncio

import pytest

from operation_dispatcher import BrowserEventVisualizer, Operation, OperationDispatcher


def populate_history(dispatcher, count):
    operation = Operation(resource_id="resource-a", payload={})
    dispatcher.add_operation(operation)
    for priority in range(1, count):
        dispatcher.update_operation(operation.id, {"priority": priority})
    return operation


@pytest.mark.parametrize("limit", [1, 3, 1200])
def test_configured_retention_keeps_latest_events_in_order(limit):
    emitted = []
    dispatcher = OperationDispatcher(
        "resource-a", event_history_limit=limit, on_notification_callback=emitted.append
    )
    populate_history(dispatcher, limit + 2)
    assert dispatcher.event_history_limit == limit
    history = dispatcher.get_history()
    assert [event.id for event in history.events] == [
        event.id for event in emitted[-limit:]
    ]
    assert dispatcher.get_history(limit=1).events == history.events[-1:]


def test_default_retention_remains_1000():
    dispatcher = OperationDispatcher("resource-a")
    populate_history(dispatcher, 1002)
    assert dispatcher.event_history_limit == 1000
    assert len(dispatcher.get_history().events) == 1000


@pytest.mark.parametrize("limit", [0, -1, 1.5, "1000", None, True, False])
def test_retention_rejects_invalid_values(limit):
    with pytest.raises(
        ValueError, match="event_history_limit must be a positive integer"
    ):
        OperationDispatcher("resource-a", event_history_limit=limit)


def test_retention_is_global_and_does_not_remove_archived_operations():
    dispatcher = OperationDispatcher("resource-a", event_history_limit=1)
    completed = populate_history(dispatcher, 1)
    asyncio.run(dispatcher.step_dispatch())
    dispatcher.complete_operation(completed.id)
    latest = populate_history(dispatcher, 1)
    assert dispatcher.get_operation(completed.id).id == completed.id
    history = dispatcher.get_history(resolve_operations=True)
    assert [event.operation_id for event in history.events] == [latest.id]
    assert history.operations == [latest]


@pytest.mark.parametrize("limit", [3, 1200])
def test_http_history_queries_and_schema_use_configured_retention(limit):
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flasgger import Swagger
    from flask import Flask

    from operation_dispatcher import OperationDispatcherOpenAPI

    dispatcher = OperationDispatcher("resource-a", event_history_limit=limit)
    operation = populate_history(dispatcher, limit + 2)
    api = OperationDispatcherOpenAPI(dispatcher)
    app = Flask(__name__)
    api.register_default_endpoints(app)
    Swagger(app, template={"definitions": api.get_openapi_definitions()})
    client = app.test_client()
    response = client.get(f"/operations/history?limit={limit}")
    assert response.status_code == 200
    assert len(response.json["events"]) == limit
    default = client.get("/operations/history")
    assert default.status_code == 200
    assert len(default.json["events"]) == min(50, limit)
    rejected = client.get(f"/operations/history?limit={limit + 1}")
    assert rejected.status_code == 400
    assert rejected.json["code"] == "invalid_limit"
    assert rejected.json["message"].endswith(str(limit))
    assert len(client.get(f"/operations/{operation.id}/events").json) == limit
    spec = client.get("/apispec_1.json").json
    limit_schema = next(
        parameter
        for parameter in spec["paths"]["/operations/history"]["get"]["parameters"]
        if parameter["name"] == "limit"
    )
    assert limit_schema["maximum"] == limit
    assert limit_schema["default"] == min(50, limit)


def test_visualizer_inherits_retention_and_supports_explicit_override():
    dispatcher = OperationDispatcher("resource-a", event_history_limit=3)
    inherited = BrowserEventVisualizer(operation_dispatcher=dispatcher)
    overridden = BrowserEventVisualizer(operation_dispatcher=dispatcher, max_events=1)
    for visualizer in (inherited, overridden):
        for index in range(5):
            visualizer.publish_event(str(index))
    assert [event["event_type"] for event in inherited._get_history()] == [
        "2",
        "3",
        "4",
    ]
    assert [event["event_type"] for event in overridden._get_history()] == ["4"]
    assert BrowserEventVisualizer()._max_events == 1000
