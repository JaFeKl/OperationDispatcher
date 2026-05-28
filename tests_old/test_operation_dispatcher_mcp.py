import time

import pytest

pytest.importorskip("mcp.server.fastmcp")

from operation_dispatcher import OperationDispatcher, OperationDispatcherMCPServer


def test_mcp_server_can_manage_dispatcher_runtime() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    mcp_server = OperationDispatcherMCPServer(
        dispatcher,
        manage_dispatcher_runtime=True,
        runtime_startup_timeout_seconds=0.5,
        runtime_stop_join_timeout_seconds=1.0,
    )

    start_payload = mcp_server.start_dispatcher_runtime()

    deadline = time.time() + 0.8
    while not dispatcher.is_running and time.time() < deadline:
        time.sleep(0.01)

    assert start_payload["state"]["runtime_managed_by"] == "mcp"
    assert dispatcher.is_running is True

    stop_payload = mcp_server.stop_dispatcher_runtime()

    deadline = time.time() + 0.8
    while dispatcher.is_running and time.time() < deadline:
        time.sleep(0.01)

    assert stop_payload["state"]["runtime_managed_by"] == "mcp"
    assert dispatcher.is_running is False


def test_mcp_server_can_be_configured_for_external_runtime_owner() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    mcp_server = OperationDispatcherMCPServer(
        dispatcher,
        manage_dispatcher_runtime=False,
    )

    start_payload = mcp_server.start_dispatcher_runtime()
    stop_payload = mcp_server.stop_dispatcher_runtime()

    assert start_payload["message"] == "dispatcher runtime is managed externally"
    assert stop_payload["message"] == "dispatcher runtime is managed externally"
    assert start_payload["state"]["runtime_managed_by"] == "external"
    assert stop_payload["state"]["runtime_managed_by"] == "external"
