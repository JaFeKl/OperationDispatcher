import pytest

pytest.importorskip("mcp.server.fastmcp")

from operation_dispatcher import (
    BasicMCPTool,
    OperationDispatcher,
    OperationDispatcherMCPServer,
)


def test_mcp_server_can_manage_dispatcher_runtime() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    mcp_server = OperationDispatcherMCPServer(dispatcher)

    pause_payload = mcp_server.pause_dispatcher_runtime()
    assert pause_payload["message"] == "operation dispatcher paused"
    assert dispatcher.is_paused is True

    resume_payload = mcp_server.resume_dispatcher_runtime()
    assert resume_payload["message"] == "operation dispatcher resumed"
    assert dispatcher.is_paused is False


def test_mcp_server_can_be_configured_for_external_runtime_owner() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    mcp_server = OperationDispatcherMCPServer(dispatcher)

    state_payload = mcp_server._dispatcher_state_payload()

    assert "runtime_thread_alive" not in state_payload
    assert "runtime_last_error" not in state_payload
    assert "runtime_managed_by" not in state_payload


def test_mcp_server_enables_all_basic_tools_by_default() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    mcp_server = OperationDispatcherMCPServer(dispatcher)

    assert set(mcp_server.enabled_basic_tools) == set(BasicMCPTool)


def test_mcp_server_accepts_custom_basic_tool_subset() -> None:
    dispatcher = OperationDispatcher(resource_id="resource-a")
    selected_tools = [
        BasicMCPTool.GET_DISPATCHER_STATE,
        BasicMCPTool.PAUSE_OPERATION_DISPATCHER,
    ]
    mcp_server = OperationDispatcherMCPServer(
        dispatcher,
        basic_tools=selected_tools,
    )

    assert mcp_server.enabled_basic_tools == tuple(selected_tools)
