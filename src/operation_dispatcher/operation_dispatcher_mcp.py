from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP
from operation_dispatcher.operation_dispatcher import OperationDispatcher


@dataclass(slots=True, frozen=True)
class OperationDispatcherMCPContext:
    """Typed MCP context for tools backed by an operation dispatcher."""

    operation_dispatcher: OperationDispatcher


class BasicMCPTool(str, Enum):
    GET_DISPATCHER_STATE = "get_dispatcher_state"
    PAUSE_OPERATION_DISPATCHER = "pause_operation_dispatcher"
    RESUME_OPERATION_DISPATCHER = "resume_operation_dispatcher"


class OperationDispatcherMCPServer:
    """Reusable MCP server wrapper around a shared `OperationDispatcher`."""

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        *,
        basic_tools: list[BasicMCPTool] | None = None,
        name: str = "Operation Dispatcher",
        instructions: str | None = None,
        host: str = "127.0.0.1",
        port: int = 8000,
        json_response: bool = True,
        **fastmcp_kwargs: Any,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._enabled_basic_tools = self._resolve_basic_tools(basic_tools)

        self._mcp = FastMCP(
            name=name,
            instructions=instructions,
            host=host,
            port=port,
            json_response=json_response,
            lifespan=self._create_lifespan(),
            **fastmcp_kwargs,
        )
        self._register_base_tools()

    @staticmethod
    def _resolve_basic_tools(
        basic_tools: list[BasicMCPTool] | None,
    ) -> tuple[BasicMCPTool, ...]:
        if basic_tools is None:
            return tuple(BasicMCPTool)
        return tuple(dict.fromkeys(basic_tools))

    @property
    def operation_dispatcher(self) -> OperationDispatcher:
        return self._operation_dispatcher

    @property
    def app(self) -> FastMCP:
        return self._mcp

    @property
    def enabled_basic_tools(self) -> tuple[BasicMCPTool, ...]:
        return self._enabled_basic_tools

    def tool(self, *args: Any, **kwargs: Any) -> Any:
        return self._mcp.tool(*args, **kwargs)

    def resource(self, *args: Any, **kwargs: Any) -> Any:
        return self._mcp.resource(*args, **kwargs)

    def prompt(self, *args: Any, **kwargs: Any) -> Any:
        return self._mcp.prompt(*args, **kwargs)

    def run(
        self,
        transport: Literal["stdio", "sse", "streamable-http"] = "sse",
        mount_path: str | None = None,
    ) -> None:
        self._mcp.run(transport=transport, mount_path=mount_path)

    def _create_lifespan(self):
        @asynccontextmanager
        async def lifespan(_: FastMCP) -> AsyncIterator[OperationDispatcherMCPContext]:
            yield OperationDispatcherMCPContext(
                operation_dispatcher=self._operation_dispatcher
            )

        return lifespan

    def _dispatcher_state_payload(self) -> dict[str, Any]:
        return self._operation_dispatcher.get_state().model_dump(mode="json")

    def resume_dispatcher_runtime(self) -> dict[str, Any]:
        if not self._operation_dispatcher.is_paused:
            return {
                "message": "operation dispatcher is not paused",
                "state": self._dispatcher_state_payload(),
            }

        self._operation_dispatcher.resume_dispatcher_runtime()
        return {
            "message": "operation dispatcher resumed",
            "state": self._dispatcher_state_payload(),
        }

    def pause_dispatcher_runtime(self) -> dict[str, Any]:
        if self._operation_dispatcher.is_paused:
            return {
                "message": "operation dispatcher is already paused",
                "state": self._dispatcher_state_payload(),
            }

        self._operation_dispatcher.pause_dispatcher_runtime()
        return {
            "message": "operation dispatcher paused",
            "state": self._dispatcher_state_payload(),
        }

    def _register_base_tools(self) -> None:
        if BasicMCPTool.GET_DISPATCHER_STATE in self._enabled_basic_tools:

            @self._mcp.tool(
                name=BasicMCPTool.GET_DISPATCHER_STATE.value,
                description="Return the current state of the shared operation dispatcher.",
            )
            def get_dispatcher_state() -> dict[str, Any]:
                return self._dispatcher_state_payload()

        if BasicMCPTool.PAUSE_OPERATION_DISPATCHER in self._enabled_basic_tools:

            @self._mcp.tool(
                name=BasicMCPTool.PAUSE_OPERATION_DISPATCHER.value,
                description="Pause the operation dispatcher.",
            )
            def pause_operation_dispatcher() -> dict[str, Any]:
                return self.pause_dispatcher_runtime()

        if BasicMCPTool.RESUME_OPERATION_DISPATCHER in self._enabled_basic_tools:

            @self._mcp.tool(
                name=BasicMCPTool.RESUME_OPERATION_DISPATCHER.value,
                description="Resume the paused operation dispatcher.",
            )
            def resume_operation_dispatcher() -> dict[str, Any]:
                return self.resume_dispatcher_runtime()


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
