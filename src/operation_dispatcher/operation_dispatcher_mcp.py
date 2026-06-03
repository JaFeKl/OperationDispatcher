from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from fastmcp import FastMCP
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
        self._host = host
        self._port = port
        self._json_response = json_response

        self._mcp = FastMCP(
            name=name,
            instructions=instructions,
            lifespan=self._create_lifespan(),
            **fastmcp_kwargs,
        )
        self._register_base_tools()
        self._register_base_resources()

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
    def app(self) -> Any:
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
        transport: Literal["stdio", "sse", "streamable-http", "http"] = "sse",
        mount_path: str | None = None,
    ) -> None:
        resolved_transport = "http" if transport == "streamable-http" else transport
        run_kwargs: dict[str, Any] = {
            "transport": resolved_transport,
            "host": self._host,
            "port": self._port,
            "json_response": self._json_response,
        }
        if mount_path is not None:
            run_kwargs["mount_path"] = mount_path

        try:
            self._mcp.run(**run_kwargs)
        except TypeError:
            legacy_transport = (
                "streamable-http"
                if resolved_transport == "http"
                else resolved_transport
            )
            self._mcp.run(transport=legacy_transport, mount_path=mount_path)

    def _create_lifespan(self):
        @asynccontextmanager
        async def lifespan(_: Any) -> AsyncIterator[OperationDispatcherMCPContext]:
            yield OperationDispatcherMCPContext(
                operation_dispatcher=self._operation_dispatcher
            )

        return lifespan

    def _dispatcher_state_payload(self) -> dict[str, Any]:
        return self._operation_dispatcher.get_state().model_dump(mode="json")

    def _dispatcher_queue_payload(self) -> list[dict[str, Any]]:
        return [
            operation.model_dump(mode="json")
            for operation in self._operation_dispatcher.get_schedule()
        ]

    def _dispatcher_history_payload(self, *, limit: int = 50) -> dict[str, Any]:
        resolved_limit = max(1, min(limit, 1000))
        return self._operation_dispatcher.get_history(limit=resolved_limit).model_dump(
            mode="json"
        )

    def _dispatcher_events_payload(self, *, limit: int = 100) -> list[dict[str, Any]]:
        resolved_limit = max(1, min(limit, 1000))
        return [
            event.model_dump(mode="json")
            for event in self._operation_dispatcher.get_event_history(
                limit=resolved_limit
            )
        ]

    def _operation_payload(self, operation_id: str) -> dict[str, Any]:
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError:
            return {
                "error": "invalid_operation_id",
                "message": "operation_id must be a valid UUID",
            }

        operation = self._operation_dispatcher.get_operation(parsed_operation_id)
        if operation is None:
            return {
                "error": "operation_not_found",
                "message": "operation not found",
                "operation_id": operation_id,
            }

        return operation.model_dump(mode="json")

    def _operation_events_payload(self, operation_id: str) -> list[dict[str, Any]]:
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError:
            return [
                {
                    "error": "invalid_operation_id",
                    "message": "operation_id must be a valid UUID",
                    "operation_id": operation_id,
                }
            ]

        return [
            event.model_dump(mode="json")
            for event in self._operation_dispatcher.get_event_history()
            if event.operation_id == parsed_operation_id
        ]

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

    def _register_base_resources(self) -> None:
        @self._mcp.resource(
            "dispatcher://state",
            name="dispatcher_state",
            description="Current operation dispatcher runtime state.",
        )
        def dispatcher_state_resource() -> dict[str, Any]:
            return self._dispatcher_state_payload()

        @self._mcp.resource(
            "dispatcher://queue",
            name="dispatcher_queue",
            description="Current queued operations for the dispatcher resource.",
        )
        def dispatcher_queue_resource() -> list[dict[str, Any]]:
            return self._dispatcher_queue_payload()

        @self._mcp.resource(
            "dispatcher://history",
            name="dispatcher_history",
            description="Recent completed operation history (default limit 50).",
        )
        def dispatcher_history_resource() -> dict[str, Any]:
            return self._dispatcher_history_payload(limit=50)

        @self._mcp.resource(
            "dispatcher://events",
            name="dispatcher_events",
            description="Recent dispatcher events (default limit 100).",
        )
        def dispatcher_events_resource() -> list[dict[str, Any]]:
            return self._dispatcher_events_payload(limit=100)

        @self._mcp.resource(
            "dispatcher://operations/{operation_id}",
            name="dispatcher_operation",
            description="Lookup a single operation by UUID.",
        )
        def dispatcher_operation_resource(operation_id: str) -> dict[str, Any]:
            return self._operation_payload(operation_id)

        @self._mcp.resource(
            "dispatcher://operations/{operation_id}/events",
            name="dispatcher_operation_events",
            description="List all events emitted for a specific operation UUID.",
        )
        def dispatcher_operation_events_resource(
            operation_id: str,
        ) -> list[dict[str, Any]]:
            return self._operation_events_payload(operation_id)


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
