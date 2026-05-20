from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from .operation_dispatcher import OperationDispatcher


@dataclass(slots=True, frozen=True)
class OperationDispatcherMCPContext:
    """Typed MCP context for tools backed by an operation dispatcher."""

    operation_dispatcher: OperationDispatcher


class OperationDispatcherMCPServer:
    """Reusable MCP server wrapper around a shared `OperationDispatcher`."""

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        *,
        name: str = "Operation Dispatcher",
        instructions: str | None = None,
        host: str = "127.0.0.1",
        port: int = 8000,
        json_response: bool = True,
        **fastmcp_kwargs: Any,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
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

    @property
    def operation_dispatcher(self) -> OperationDispatcher:
        return self._operation_dispatcher

    @property
    def app(self) -> FastMCP:
        return self._mcp

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

    def _register_base_tools(self) -> None:
        @self._mcp.tool(
            name="get_dispatcher_state",
            description="Return the current state of the shared operation dispatcher.",
        )
        def get_dispatcher_state() -> dict[str, Any]:
            return self._operation_dispatcher.get_state().model_dump(mode="json")


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)