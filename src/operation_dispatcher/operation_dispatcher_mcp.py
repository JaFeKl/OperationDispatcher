from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from mcp.server.fastmcp import FastMCP
from operation_dispatcher.operation_dispatcher import OperationDispatcher
from operation_dispatcher.runtime_controller import OperationDispatcherRuntimeController

if TYPE_CHECKING:
    from operation_dispatcher.operation_dispatcher_openapi import (
        OperationDispatcherOpenAPI,
    )


@dataclass(slots=True, frozen=True)
class OperationDispatcherMCPContext:
    """Typed MCP context for tools backed by an operation dispatcher."""

    operation_dispatcher: OperationDispatcher


class OperationDispatcherMCPServer:
    """Reusable MCP server wrapper around a shared `OperationDispatcher`."""

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher | OperationDispatcherOpenAPI,
        *,
        name: str = "Operation Dispatcher",
        instructions: str | None = None,
        host: str = "127.0.0.1",
        port: int = 8000,
        json_response: bool = True,
        manage_dispatcher_runtime: bool = True,
        runtime_startup_timeout_seconds: float = 1.0,
        runtime_stop_join_timeout_seconds: float = 2.0,
        **fastmcp_kwargs: Any,
    ) -> None:
        from .operation_dispatcher_openapi import OperationDispatcherOpenAPI

        if isinstance(operation_dispatcher, OperationDispatcherOpenAPI):
            self._operation_dispatcher_api: OperationDispatcherOpenAPI | None = (
                operation_dispatcher
            )
            self._operation_dispatcher = operation_dispatcher._operation_dispatcher
            self._manage_dispatcher_runtime = False
            self._runtime_controller = operation_dispatcher.runtime_controller
        else:
            self._operation_dispatcher_api = None
            self._operation_dispatcher = operation_dispatcher
            self._manage_dispatcher_runtime = manage_dispatcher_runtime
            self._runtime_controller = OperationDispatcherRuntimeController(
                operation_dispatcher=operation_dispatcher,
                startup_timeout_seconds=runtime_startup_timeout_seconds,
                stop_join_timeout_seconds=runtime_stop_join_timeout_seconds,
            )

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
    def manages_dispatcher_runtime(self) -> bool:
        return self._manage_dispatcher_runtime

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
            try:
                yield OperationDispatcherMCPContext(
                    operation_dispatcher=self._operation_dispatcher
                )
            finally:
                if self._manage_dispatcher_runtime:
                    self.stop_dispatcher_runtime()

        return lifespan

    def _dispatcher_state_payload(self) -> dict[str, Any]:
        if self._operation_dispatcher_api is not None:
            state, _ = (
                self._operation_dispatcher_api.get_operation_dispatcher_state_response()
            )
            state = dict(state)
            state["runtime_managed_by"] = "openapi"
            return state

        state = self._runtime_controller.get_state_payload()
        state["runtime_managed_by"] = (
            "mcp" if self._manage_dispatcher_runtime else "external"
        )
        return state

    def start_dispatcher_runtime(self) -> dict[str, Any]:
        if self._operation_dispatcher_api is not None:
            payload, status_code = (
                self._operation_dispatcher_api.start_operation_dispatcher_response()
            )
            return {
                "status_code": status_code,
                "response": payload,
            }

        if not self._manage_dispatcher_runtime:
            return {
                "message": "dispatcher runtime is managed externally",
                "state": self._dispatcher_state_payload(),
            }

        payload, _ = self._runtime_controller.start()
        payload = dict(payload)
        payload["state"] = self._dispatcher_state_payload()
        return payload

    def stop_dispatcher_runtime(self) -> dict[str, Any]:
        if self._operation_dispatcher_api is not None:
            payload, status_code = (
                self._operation_dispatcher_api.stop_operation_dispatcher_response()
            )
            return {
                "status_code": status_code,
                "response": payload,
            }

        if not self._manage_dispatcher_runtime:
            return {
                "message": "dispatcher runtime is managed externally",
                "state": self._dispatcher_state_payload(),
            }

        payload, _ = self._runtime_controller.stop()
        payload = dict(payload)
        payload["state"] = self._dispatcher_state_payload()
        return payload

    def resume_dispatcher_runtime(self) -> dict[str, Any]:
        if self._operation_dispatcher_api is not None:
            payload, status_code = (
                self._operation_dispatcher_api.resume_operation_dispatcher_response()
            )
            return {
                "status_code": status_code,
                "response": payload,
            }

        payload, _ = self._runtime_controller.resume()
        return payload

    def pause_dispatcher_runtime(self) -> dict[str, Any]:
        if self._operation_dispatcher_api is not None:
            payload, status_code = (
                self._operation_dispatcher_api.pause_operation_dispatcher_response()
            )
            return {
                "status_code": status_code,
                "response": payload,
            }

        payload, _ = self._runtime_controller.pause()
        return payload

    def _register_base_tools(self) -> None:
        @self._mcp.tool(
            name="get_dispatcher_state",
            description="Return the current state of the shared operation dispatcher.",
        )
        def get_dispatcher_state() -> dict[str, Any]:
            return self._dispatcher_state_payload()

        @self._mcp.tool(
            name="start_operation_dispatcher",
            description="Start the operation dispatcher.",
        )
        def start_operation_dispatcher() -> dict[str, Any]:
            return self.start_dispatcher_runtime()

        @self._mcp.tool(
            name="stop_operation_dispatcher",
            description="Stop the operation dispatcher.",
        )
        def stop_operation_dispatcher() -> dict[str, Any]:
            return self.stop_dispatcher_runtime()

        @self._mcp.tool(
            name="pause_operation_dispatcher",
            description="Pause the operation dispatcher.",
        )
        def pause_operation_dispatcher() -> dict[str, Any]:
            return self.pause_dispatcher_runtime()

        @self._mcp.tool(
            name="resume_operation_dispatcher",
            description="Resume the paused operation dispatcher.",
        )
        def resume_operation_dispatcher() -> dict[str, Any]:
            return self.resume_dispatcher_runtime()


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher | OperationDispatcherOpenAPI,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
