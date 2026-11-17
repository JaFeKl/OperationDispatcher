import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from uuid import UUID

import pytest

pytest.importorskip("fastmcp")
from fastmcp import Client

from operation_dispatcher import (
    DEFAULT_MCP_TOOLS,
    DispatcherMCPTools,
    EventType,
    OperationDispatcher,
    OperationDispatcherAPI,
    OperationDispatcherMCPServer,
    OperationDispatcherRuntimeController,
    create_operation_dispatcher_mcp_server,
    create_operation_dispatcher_mcp_utility,
)


def payload(result):
    # Compare wire data; FastMCP 2.x may decode result.data into typed objects.
    structured = result.structured_content
    return structured["result"] if set(structured) == {"result"} else structured


@pytest.mark.parametrize("limit", [3, 1200])
def test_mcp_history_tools_resources_and_prompt_use_configured_retention(limit):
    async def scenario():
        dispatcher = OperationDispatcher("resource-a", event_history_limit=limit)
        api = OperationDispatcherAPI(dispatcher)
        operation_id = api.add_operation_response([{"payload": {}}])[0][0]["operation"][
            "id"
        ]
        for priority in range(1, limit + 2):
            api.update_operation_response(operation_id, {"priority": priority})
        async with Client(OperationDispatcherMCPServer(dispatcher).app) as client:
            history = payload(
                await client.call_tool("dispatcher_get_history", {"limit": limit})
            )
            assert len(history["events"]) == limit
            default = payload(await client.call_tool("dispatcher_get_history"))
            assert len(default["events"]) == min(50, limit)
            rejected = await client.call_tool(
                "dispatcher_get_history", {"limit": limit + 1}, raise_on_error=False
            )
            assert rejected.is_error
            assert json.loads(rejected.content[0].text)["code"] == "invalid_limit"
            for uri in (
                f"dispatcher://history/{limit}",
                f"dispatcher://events/{limit}",
                f"dispatcher://operations/session/{operation_id}/events",
            ):
                resource = await client.read_resource(uri)
                assert json.loads(resource[0].text)["events"] == history["events"]
            prompt = await client.get_prompt("dispatcher_introduction_prompt")
            assert f"maximum {limit}" in prompt.messages[0].content.text

    asyncio.run(scenario())


def test_mcp_discovery_and_custom_selection():
    async def scenario():
        dispatcher = OperationDispatcher("resource-a")
        server = create_operation_dispatcher_mcp_server(dispatcher)
        assert (
            create_operation_dispatcher_mcp_utility(dispatcher).operation_dispatcher
            is dispatcher
        )
        async with Client(server.app) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
            assert set(tools) == {tool.value for tool in DEFAULT_MCP_TOOLS}
            assert len(tools) == 14
            # The thin wrapper must preserve discoverable, useful parameters.
            tool = tools["dispatcher_update_operation"]
            schema = (
                tool.input_schema if hasattr(tool, "input_schema") else tool.inputSchema
            )
            assert set(schema["required"]) == {"operation_id", "updates_payload"}
            assert schema["properties"]["updates_payload"]["type"] == "object"
            assert set(schema["properties"]) == {"operation_id", "updates_payload"}
            assert len(await client.list_resources()) == 2
            assert len(await client.list_resource_templates()) == 4
            prompt = await client.get_prompt("dispatcher_introduction_prompt")
            assert "SUCCESS or FAILURE" in prompt.messages[0].content.text

        for selected in ([], [DispatcherMCPTools.GET_OPERATION]):
            server = OperationDispatcherMCPServer(
                dispatcher, tools=selected, resources=[], prompts=[]
            )
            async with Client(server.app) as client:
                assert {tool.name for tool in await client.list_tools()} == {
                    tool.value for tool in selected
                }
                assert await client.list_resources() == []
                assert await client.list_resource_templates() == []
                assert await client.list_prompts() == []

    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", ["SUCCESS", "FAILURE"])
def test_mcp_operation_flow_uses_shared_api_payloads(outcome):
    async def scenario():
        dispatcher = OperationDispatcher(
            "resource-a", on_request_callback=lambda event: True
        )
        api = OperationDispatcherAPI(dispatcher)
        async with Client(OperationDispatcherMCPServer(dispatcher).app) as client:

            async def call(name, **kwargs):
                return payload(await client.call_tool(name, kwargs))

            # Empty arrays must survive MCP serialization as arrays.
            assert await call("dispatcher_list_operations") == []
            added = await call(
                "dispatcher_add_operations",
                request_payload=[
                    {"payload": {"task": "first"}, "priority": 2},
                    {"payload": {"task": "second"}},
                ],
            )
            first, second = [item["operation"]["id"] for item in added]
            assert (
                await call("dispatcher_get_operation", operation_id=first)
                == api.get_operation_response(first)[0]
            )
            updated = await call(
                "dispatcher_update_operation",
                operation_id=first,
                updates_payload={"priority": 3},
            )
            assert updated["operation"]["priority"] == 3
            assert (
                await call("dispatcher_list_operations", state="QUEUED")
                == api.list_operations_response("QUEUED")[0]
            )

            await dispatcher.step_dispatch()
            assert (
                await call("dispatcher_get_current_operation")
                == api.get_current_operation_response()[0]
            )
            paused = await call(
                "dispatcher_pause_operation",
                operation_id=first,
                command_payload={"meta_data": {"source": "mcp"}},
            )
            assert paused["operation"]["state"] == "PAUSED"
            resumed = await call("dispatcher_resume_operation", operation_id=first)
            assert resumed["operation"]["state"] == "RUNNING"
            command = {"outcome": outcome, "meta_data": {"source": "mcp"}}
            if outcome == "FAILURE":
                command["termination_reason"] = "EXTERNAL_ERROR"
            completed = await call(
                "dispatcher_request_operation_completion",
                operation_id=first,
                command_payload=command,
            )
            assert completed["operation"]["state"] == "COMPLETED"
            assert completed["operation"]["outcome"] == outcome
            assert completed["is_current_operation"] is False
            if outcome == "FAILURE":
                assert completed["operation"]["termination_reason"] == "EXTERNAL_ERROR"
            assert (
                await call("dispatcher_get_operation_events", operation_id=first)
                == api.get_operation_events_response(first)[0]
            )
            actual = await call("dispatcher_get_history", limit=10)
            expected = api.get_history_response(limit=10)[0]
            # Each history query creates a fresh response timestamp.
            actual.pop("created_at")
            expected.pop("created_at")
            assert actual == expected

            cancelled = await call("dispatcher_cancel_operation", operation_id=second)
            assert cancelled["operation"]["outcome"] == "CANCELLED"
            assert await call("dispatcher_list_operations") == []

            # Existing resource URIs and successful payload shapes are retained.
            resource = await client.read_resource(
                f"dispatcher://operations/session/{first}"
            )
            assert json.loads(resource[0].text) == completed["operation"]
            resource = await client.read_resource(
                f"dispatcher://operations/session/{first}/events"
            )
            assert (
                json.loads(resource[0].text)["events"]
                == api.get_operation_events_response(first)[0]
            )
            resource = await client.read_resource("dispatcher://queue")
            assert json.loads(resource[0].text) == {"operations": []}

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "tool,arguments,method,path,body",
    [
        (
            "dispatcher_get_operation",
            {"operation_id": "invalid"},
            "get",
            "/operations/invalid",
            None,
        ),
        (
            "dispatcher_get_operation_events",
            {"operation_id": "00000000-0000-0000-0000-000000000001"},
            "get",
            "/operations/00000000-0000-0000-0000-000000000001/events",
            None,
        ),
        (
            "dispatcher_list_operations",
            {"state": "COMPLETED"},
            "get",
            "/operations?state=COMPLETED",
            None,
        ),
        (
            "dispatcher_get_history",
            {"limit": 1001},
            "get",
            "/operations/history?limit=1001",
            None,
        ),
        (
            "dispatcher_get_history",
            {"from_time": "2026-02-02T00:00:00Z", "to_time": "2026-01-01T00:00:00Z"},
            "get",
            "/operations/history?from_time=2026-02-02T00:00:00Z&to_time=2026-01-01T00:00:00Z",
            None,
        ),
        (
            "dispatcher_add_operations",
            {"request_payload": []},
            "post",
            "/operations/add",
            [],
        ),
        (
            "dispatcher_request_operation_completion",
            {"operation_id": "invalid", "command_payload": {"outcome": "CANCELLED"}},
            "post",
            "/operations/invalid/complete",
            {"outcome": "CANCELLED"},
        ),
    ],
)
def test_mcp_errors_match_http_payloads(tool, arguments, method, path, body):
    pytest.importorskip("flask")
    pytest.importorskip("flasgger")
    from flask import Flask
    from operation_dispatcher import OperationDispatcherOpenAPI

    async def scenario():
        dispatcher = OperationDispatcher("resource-a")
        app = Flask(__name__)
        OperationDispatcherOpenAPI(dispatcher).register_default_endpoints(app)
        response = app.test_client().open(path, method=method.upper(), json=body)
        assert response.status_code >= 400
        async with Client(OperationDispatcherMCPServer(dispatcher).app) as client:
            result = await client.call_tool(tool, arguments, raise_on_error=False)
            assert result.is_error
            assert json.loads(result.content[0].text) == response.json

    asyncio.run(scenario())


def test_mcp_approval_does_not_block_queries_and_preserves_denial():
    entered, release = threading.Event(), threading.Event()

    def approve(event):
        if event.event_type is EventType.OPERATION_COMPLETE_REQUESTED:
            entered.set()
            release.wait(3)
            return False
        return True

    async def scenario():
        dispatcher = OperationDispatcher("resource-a", on_request_callback=approve)
        async with Client(OperationDispatcherMCPServer(dispatcher).app) as client:
            result = await client.call_tool(
                "dispatcher_add_operations", {"request_payload": [{"payload": {}}]}
            )
            operation_id = payload(result)[0]["operation"]["id"]
            await dispatcher.step_dispatch()
            pending = asyncio.create_task(
                client.call_tool(
                    "dispatcher_request_operation_completion",
                    {
                        "operation_id": operation_id,
                        "command_payload": {"outcome": "SUCCESS"},
                    },
                    raise_on_error=False,
                )
            )
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                state = await asyncio.wait_for(
                    client.call_tool("dispatcher_get_state"), 1
                )
                assert payload(state)["current_operation"]["state"] == "RUNNING"
            finally:
                release.set()
            result = await pending
            assert result.is_error
            assert (
                json.loads(result.content[0].text)["code"]
                == "operation_completion_denied"
            )
            assert dispatcher.get_operation(UUID(operation_id)).state.value == "RUNNING"

    asyncio.run(scenario())


def test_optional_runtime_tools_use_supplied_controller():
    async def scenario():
        dispatcher = OperationDispatcher("resource-a")
        controller = OperationDispatcherRuntimeController(dispatcher)
        server = OperationDispatcherMCPServer(
            dispatcher, tools=list(DispatcherMCPTools), runtime_controller=controller
        )
        try:
            async with Client(server.app) as client:
                started = await client.call_tool("dispatcher_start_runtime")
                assert payload(started)["state"]["is_running"] is True
                assert payload(started)["state"]["runtime_thread_alive"] is True
                assert controller.runtime_thread is not None
                await client.call_tool("dispatcher_pause_runtime")
                duplicate = await client.call_tool(
                    "dispatcher_pause_runtime", raise_on_error=False
                )
                assert duplicate.is_error
                await client.call_tool("dispatcher_resume_runtime")
                await client.call_tool("dispatcher_stop_runtime")
                assert not dispatcher.is_running
        finally:
            await asyncio.to_thread(controller.stop)

    asyncio.run(scenario())


def test_mcp_import_and_registration_without_flask():
    script = """
import importlib.abc
import sys
class BlockHTTP(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"flask", "flasgger"}:
            raise ModuleNotFoundError(fullname)
sys.meta_path.insert(0, BlockHTTP())
from operation_dispatcher import OperationDispatcher, OperationDispatcherAPI, OperationDispatcherMCPServer
OperationDispatcherMCPServer(OperationDispatcher("resource-a"))
assert "flask" not in sys.modules
assert "flasgger" not in sys.modules
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        check=True,
        timeout=15,
        capture_output=True,
    )


@pytest.mark.parametrize("transport", ["stdio", "http", "sse", "streamable-http"])
def test_server_passes_only_transport_specific_arguments(monkeypatch, transport):
    server = OperationDispatcherMCPServer(OperationDispatcher("resource-a"))
    calls = []
    monkeypatch.setattr(server.app, "run", lambda **kwargs: calls.append(kwargs))
    server.run(
        transport=transport, mount_path=None if transport == "stdio" else "/custom"
    )
    if transport == "stdio":
        assert calls == [{"transport": "stdio"}]
    else:
        assert calls == [
            {
                "transport": "http" if transport == "streamable-http" else transport,
                "host": "127.0.0.1",
                "port": 8000,
                "json_response": True,
                "path": "/custom",
            }
        ]


def test_server_does_not_retry_internal_type_errors(monkeypatch):
    server = OperationDispatcherMCPServer(OperationDispatcher("resource-a"))
    calls = []

    def broken(**kwargs):
        calls.append(kwargs)
        raise TypeError("internal transport failure")

    monkeypatch.setattr(server.app, "run", broken)
    with pytest.raises(TypeError, match="internal transport failure"):
        server.run()
    assert len(calls) == 1


def test_stdio_server_starts_and_exits_cleanly_on_eof():
    import operation_dispatcher

    # Use the same package as the test process, including installed-wheel CI runs.
    env = dict(
        os.environ, PYTHONPATH=str(Path(operation_dispatcher.__file__).parent.parent)
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from operation_dispatcher import OperationDispatcher, OperationDispatcherMCPServer; OperationDispatcherMCPServer(OperationDispatcher('resource-a')).run(transport='stdio')",
        ],
        input="",
        text=True,
        env=env,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
