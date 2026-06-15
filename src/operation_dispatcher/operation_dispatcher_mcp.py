from __future__ import annotations

from datetime import datetime
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from fastmcp import FastMCP
from fastmcp.prompts import Message
from operation_dispatcher.operation_dispatcher import OperationDispatcher


@dataclass(slots=True, frozen=True)
class OperationDispatcherMCPContext:
    """Typed MCP context for tools backed by an operation dispatcher."""

    operation_dispatcher: OperationDispatcher


class DispatcherMCPTools(str, Enum):
    GET_DISPATCHER_STATE = "dispatcher_get_state"
    PAUSE_OPERATION_DISPATCHER = "dispatcher_pause_runtime"
    RESUME_OPERATION_DISPATCHER = "dispatcher_resume_runtime"
    GET_HISTORY = "dispatcher_get_history"


class DispatcherMCPResources(str, Enum):
    DISPATCHER_STATE = "dispatcher://state"
    DISPATCHER_QUEUE = "dispatcher://queue"
    DISPATCHER_HISTORY = "dispatcher://history/{limit}"
    DISPATCHER_EVENTS = "dispatcher://events/{limit}"
    DISPATCHER_OPERATION = "dispatcher://operations/session/{operation_id}"
    DISPATCHER_OPERATION_EVENTS = (
        "dispatcher://operations/session/{operation_id}/events"
    )


class DispatcherMCPPrompts(str, Enum):
    DISPATCHER_INTRODUCTION = "dispatcher_introduction_prompt"


class OperationDispatcherMCPUtility:
    """Register dispatcher MCP tools/resources/prompts onto an existing `FastMCP` app."""

    def __init__(self, operation_dispatcher: OperationDispatcher) -> None:
        self._operation_dispatcher = operation_dispatcher

    @property
    def operation_dispatcher(self) -> OperationDispatcher:
        return self._operation_dispatcher

    def register(
        self,
        mcp: FastMCP,
        tools: list[DispatcherMCPTools] | None = None,
        resources: list[DispatcherMCPResources] | None = None,
        prompts: list[DispatcherMCPPrompts] | None = None,
    ) -> None:
        resolved_tools = tools if tools is not None else list(DispatcherMCPTools)
        resolved_resources = (
            resources if resources is not None else list(DispatcherMCPResources)
        )
        resolved_prompts = (
            prompts
            if prompts is not None
            else [DispatcherMCPPrompts.DISPATCHER_INTRODUCTION]
        )

        self.register_tools(mcp, resolved_tools)
        self.register_resources(mcp, resolved_resources)
        self.register_prompts(mcp, resolved_prompts)

    def register_tools(self, mcp: FastMCP, tools: list[DispatcherMCPTools]) -> None:
        if DispatcherMCPTools.GET_DISPATCHER_STATE in tools:

            @mcp.tool(
                name=DispatcherMCPTools.GET_DISPATCHER_STATE.value,
                description="Return the current state of the shared operation dispatcher.",
            )
            def get_dispatcher_state() -> dict[str, Any]:
                return self._dispatcher_state_payload()

        if DispatcherMCPTools.PAUSE_OPERATION_DISPATCHER in tools:

            @mcp.tool(
                name=DispatcherMCPTools.PAUSE_OPERATION_DISPATCHER.value,
                description="Pause the operation dispatcher runtime loop.",
            )
            def pause_operation_dispatcher() -> dict[str, Any]:
                return self.pause_dispatcher_runtime()

        if DispatcherMCPTools.RESUME_OPERATION_DISPATCHER in tools:

            @mcp.tool(
                name=DispatcherMCPTools.RESUME_OPERATION_DISPATCHER.value,
                description="Resume the paused operation dispatcher runtime loop.",
            )
            def resume_operation_dispatcher() -> dict[str, Any]:
                return self.resume_dispatcher_runtime()

        if DispatcherMCPTools.GET_HISTORY in tools:

            @mcp.tool(
                name=DispatcherMCPTools.GET_HISTORY.value,
                description="Get a historical slice of dispatcher events and optionally resolved operations. Can be filtered by time range and limited in size (default limit 1000, max limit 10000).",
            )
            def dispatcher_history_resource(
                from_time: datetime | None = None,
                to_time: datetime | None = None,
                resolve_operations: bool = True,
                limit: int | None = None,
            ) -> dict[str, Any]:
                return self._dispatcher_history_payload(
                    from_time=from_time,
                    to_time=to_time,
                    resolve_operations=resolve_operations,
                    limit=limit,
                )

    def register_resources(
        self,
        mcp: FastMCP,
        resources: list[DispatcherMCPResources],
    ) -> None:
        for resource in resources:
            if resource == DispatcherMCPResources.DISPATCHER_STATE:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_STATE.value,
                    name="dispatcher_state",
                    description="Current operation dispatcher runtime state.",
                )
                def dispatcher_state_resource() -> dict[str, Any]:
                    return self._dispatcher_state_payload()

            elif resource == DispatcherMCPResources.DISPATCHER_QUEUE:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_QUEUE.value,
                    name="dispatcher_queue",
                    description="Current queued operations for the dispatcher resource.",
                )
                def dispatcher_queue_resource() -> dict[str, Any]:
                    return {"operations": self._dispatcher_queue_payload()}

            elif resource == DispatcherMCPResources.DISPATCHER_HISTORY:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_HISTORY.value,
                    name="dispatcher_history",
                    description="The most recent history including recent dispatcher events and optionally resolved operations.",
                )
                def dispatcher_history_resource(limit: int) -> dict[str, Any]:
                    return self._dispatcher_history_payload(
                        from_time=None,
                        to_time=None,
                        resolve_operations=True,
                        limit=limit,
                    )

            elif resource == DispatcherMCPResources.DISPATCHER_EVENTS:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_EVENTS.value,
                    name="dispatcher_events",
                    description="Get the most recent dispatcher events.",
                )
                def dispatcher_events_resource(limit: int) -> dict[str, Any]:
                    return {"events": self._dispatcher_events_payload(limit=limit)}

            elif resource == DispatcherMCPResources.DISPATCHER_OPERATION:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_OPERATION.value,
                    name="dispatcher_operation",
                    description="Lookup a single operation by UUID.",
                )
                def dispatcher_operation_resource(operation_id: str) -> dict[str, Any]:
                    return self._operation_payload(operation_id)

            elif resource == DispatcherMCPResources.DISPATCHER_OPERATION_EVENTS:

                @mcp.resource(
                    DispatcherMCPResources.DISPATCHER_OPERATION_EVENTS.value,
                    name="dispatcher_operation_events",
                    description="List all events emitted for a specific operation UUID.",
                )
                def dispatcher_operation_events_resource(
                    operation_id: str,
                ) -> dict[str, Any]:
                    return {
                        "operation_id": operation_id,
                        "events": self._operation_events_payload(operation_id),
                    }

    def register_prompts(
        self,
        mcp: FastMCP,
        prompts: list[DispatcherMCPPrompts],
    ) -> None:
        for prompt in prompts:
            if prompt == DispatcherMCPPrompts.DISPATCHER_INTRODUCTION:

                @mcp.prompt(
                    name=DispatcherMCPPrompts.DISPATCHER_INTRODUCTION.value,
                    description=(
                        "System-level introduction for agents using OperationDispatcher tools."
                    ),
                )
                def dispatcher_introduction_prompt() -> list[Message]:
                    return [
                        Message(
                            role="user",
                            content=self.build_dispatcher_introduction_prompt(),
                        )
                    ]

    def build_dispatcher_introduction_prompt(self) -> str:
        return (
            "OperationDispatcher overview:\n"
            "- The dispatcher manages and schedules operations for one resource_id.\n"
            "- Each operation has payload, priority, optional timing constraints, and lifecycle state.\n"
            "- A runtime loop selects eligible work and emits start/cancel/pause/resume request events.\n"
            "- External executors acknowledge request events; outcomes are fed back to complete or fail operations.\n"
            "- The dispatcher records event history and exposes schedule/state for diagnostics and auditability.\n"
            "General working principle:\n"
            "1) Operations are added and updated in the queue.\n"
            "2) An automated sorting strategy determines the order of execution.\n"
            "3) If an operation is ready for execution, the dispatcher emits a request event.\n"
            "4) External business logic needs to consume these events and report lifecycle transitions back.\n"
            "5) The dispatcher allows to manage both the lifecycle and execution of operation as well as the runtime of the dispatcher itself.\n"
            "Agent guidance:\n"
            "- Prefer read tools/resources first (state, queue, history, events) before mutating behavior.\n"
            "- When pausing or resuming runtime, verify effect by reading state again.\n"
            "- Use operation-specific history/events when diagnosing a single operation."
        )

    def _dispatcher_state_payload(self) -> dict[str, Any]:
        return self._operation_dispatcher.get_state().model_dump(mode="json")

    def _dispatcher_queue_payload(self) -> list[dict[str, Any]]:
        return [
            operation.model_dump(mode="json")
            for operation in self._operation_dispatcher.get_schedule()
        ]

    def _dispatcher_history_payload(
        self,
        from_time: datetime | None,
        to_time: datetime | None,
        resolve_operations: bool = True,
        limit: int = 1000,
    ) -> dict[str, Any]:
        return self._operation_dispatcher.get_history(
            from_time=from_time,
            to_time=to_time,
            resolve_operations=resolve_operations,
            limit=limit,
        ).model_dump(mode="json")

    def _dispatcher_events_payload(self, limit: int) -> list[dict[str, Any]]:
        history = self._operation_dispatcher.get_history(
            from_time=None,
            to_time=None,
            resolve_operations=False,
            limit=limit,
        )
        return [event.model_dump(mode="json") for event in history.events]

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
            for event in self._operation_dispatcher._event_service.get_events_for_operation(
                parsed_operation_id
            )
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


class OperationDispatcherMCPServer:
    """MCP runtime wrapper that composes `OperationDispatcherMCPUtility`."""

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        *,
        name: str = "Operation Dispatcher",
        instructions: str | None = None,
        host: str = "127.0.0.1",
        port: int = 8000,
        tools: list[DispatcherMCPTools] | None = None,
        resources: list[DispatcherMCPResources] | None = None,
        prompts: list[DispatcherMCPPrompts] | None = None,
        json_response: bool = True,
        **fastmcp_kwargs: Any,
    ) -> None:
        self._utility = OperationDispatcherMCPUtility(operation_dispatcher)
        self._host = host
        self._port = port
        self._json_response = json_response

        self._mcp = FastMCP(
            name=name,
            instructions=instructions,
            lifespan=self._create_lifespan(),
            **fastmcp_kwargs,
        )
        self._utility.register(
            self._mcp,
            tools=tools,
            resources=resources,
            prompts=prompts,
        )

    @property
    def operation_dispatcher(self) -> OperationDispatcher:
        return self._utility.operation_dispatcher

    @property
    def utility(self) -> OperationDispatcherMCPUtility:
        return self._utility

    @property
    def app(self) -> Any:
        return self._mcp

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
                operation_dispatcher=self.operation_dispatcher
            )

        return lifespan

    def _dispatcher_state_payload(self) -> dict[str, Any]:
        return self._utility._dispatcher_state_payload()

    def pause_dispatcher_runtime(self) -> dict[str, Any]:
        return self._utility.pause_dispatcher_runtime()

    def resume_dispatcher_runtime(self) -> dict[str, Any]:
        return self._utility.resume_dispatcher_runtime()


def create_operation_dispatcher_mcp_utility(
    operation_dispatcher: OperationDispatcher,
) -> OperationDispatcherMCPUtility:
    """Build MCP utility helpers around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPUtility(operation_dispatcher)


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
