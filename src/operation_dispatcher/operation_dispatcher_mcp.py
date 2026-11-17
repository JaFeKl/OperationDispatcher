from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from inspect import signature
from typing import Any, Literal, get_args

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.prompts import Message

from .action_descriptions import ACTION_DESCRIPTIONS
from .operation_dispatcher import OperationDispatcher
from .operation_dispatcher_api import OperationDispatcherAPI
from .runtime_controller import OperationDispatcherRuntimeController


@dataclass(slots=True, frozen=True)
class OperationDispatcherMCPContext:
    """Typed MCP context for tools backed by an operation dispatcher."""

    operation_dispatcher: OperationDispatcher


class DispatcherMCPTools(str, Enum):
    GET_DISPATCHER_STATE = "dispatcher_get_state"
    PAUSE_OPERATION_DISPATCHER = "dispatcher_pause_runtime"
    RESUME_OPERATION_DISPATCHER = "dispatcher_resume_runtime"
    GET_HISTORY = "dispatcher_get_history"
    LIST_OPERATIONS = "dispatcher_list_operations"
    GET_OPERATION = "dispatcher_get_operation"
    GET_CURRENT_OPERATION = "dispatcher_get_current_operation"
    GET_OPERATION_EVENTS = "dispatcher_get_operation_events"
    ADD_OPERATIONS = "dispatcher_add_operations"
    UPDATE_OPERATION = "dispatcher_update_operation"
    CANCEL_OPERATION = "dispatcher_cancel_operation"
    PAUSE_OPERATION = "dispatcher_pause_operation"
    RESUME_OPERATION = "dispatcher_resume_operation"
    REQUEST_OPERATION_COMPLETION = "dispatcher_request_operation_completion"
    START_OPERATION_DISPATCHER = "dispatcher_start_runtime"
    STOP_OPERATION_DISPATCHER = "dispatcher_stop_runtime"


DEFAULT_MCP_TOOLS = tuple(
    tool
    for tool in DispatcherMCPTools
    if tool
    not in {
        DispatcherMCPTools.START_OPERATION_DISPATCHER,
        DispatcherMCPTools.STOP_OPERATION_DISPATCHER,
    }
)


def _mcp_payload(response: tuple[Any, int]) -> Any:
    """Keep API payloads intact; report unsuccessful statuses as MCP tool errors."""
    payload, status = response
    if status >= 400:
        raise ToolError(json.dumps(payload))
    return payload


def _as_mcp_tool(handler: Callable[..., tuple[Any, int]]) -> Callable[..., Any]:
    @wraps(handler)
    async def call(**kwargs: Any) -> Any:
        # Approval callbacks and runtime control can block. Keep the MCP loop free.
        return _mcp_payload(await asyncio.to_thread(handler, **kwargs))

    # Reuse the handler's parameters for discovery; the transport removes status.
    handler_signature = signature(handler, eval_str=True)
    payload_type = get_args(handler_signature.return_annotation)[0]
    tool_signature = handler_signature.replace(return_annotation=payload_type)
    call.__signature__ = tool_signature
    call.__annotations__ = {
        name: parameter.annotation
        for name, parameter in tool_signature.parameters.items()
    }
    call.__annotations__["return"] = payload_type
    return call


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

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        runtime_controller: OperationDispatcherRuntimeController | None = None,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._api = OperationDispatcherAPI(operation_dispatcher, runtime_controller)

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
        resolved_tools = tools if tools is not None else list(DEFAULT_MCP_TOOLS)
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
        handlers = {
            DispatcherMCPTools.GET_DISPATCHER_STATE: self._api.get_operation_dispatcher_state_response,
            DispatcherMCPTools.PAUSE_OPERATION_DISPATCHER: self._api.pause_operation_dispatcher_response,
            DispatcherMCPTools.RESUME_OPERATION_DISPATCHER: self._api.resume_operation_dispatcher_response,
            DispatcherMCPTools.GET_HISTORY: self._api.get_history_response,
            DispatcherMCPTools.LIST_OPERATIONS: self._api.list_operations_response,
            DispatcherMCPTools.GET_OPERATION: self._api.get_operation_response,
            DispatcherMCPTools.GET_CURRENT_OPERATION: self._api.get_current_operation_response,
            DispatcherMCPTools.GET_OPERATION_EVENTS: self._api.get_operation_events_response,
            DispatcherMCPTools.ADD_OPERATIONS: self._api.add_operation_response,
            DispatcherMCPTools.UPDATE_OPERATION: self._api.update_operation_response,
            DispatcherMCPTools.CANCEL_OPERATION: self._api.cancel_operation_response,
            DispatcherMCPTools.PAUSE_OPERATION: self._api.pause_operation_response,
            DispatcherMCPTools.RESUME_OPERATION: self._api.resume_operation_response,
            DispatcherMCPTools.REQUEST_OPERATION_COMPLETION: self._api.complete_operation_response,
            DispatcherMCPTools.START_OPERATION_DISPATCHER: self._api.start_operation_dispatcher_response,
            DispatcherMCPTools.STOP_OPERATION_DISPATCHER: self._api.stop_operation_dispatcher_response,
        }
        for tool in tools:
            handler = handlers[tool]
            action = handler.__name__.removesuffix("_response")
            if tool is DispatcherMCPTools.GET_DISPATCHER_STATE:
                action = "get_dispatcher_state"
            mcp.tool(
                name=tool.value, description=ACTION_DESCRIPTIONS[action].description
            )(_as_mcp_tool(handler))

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
            "- Lifecycle tools request application approval for cancel/pause/resume/completion.\n"
            "- Completion takes command_payload with outcome SUCCESS or FAILURE; a configured approval callback is required.\n"
            "- Tool payloads and validation match OpenAPI. "
            f"History defaults to {min(50, self.operation_dispatcher.event_history_limit)} events "
            f"(maximum {self.operation_dispatcher.event_history_limit}).\n"
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
        return _mcp_payload(self._api.get_operation_dispatcher_state_response())

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
        limit: int | None = None,
    ) -> dict[str, Any]:
        return _mcp_payload(
            self._api.get_history_response(
                from_time, to_time, resolve_operations, limit
            )
        )

    def _dispatcher_events_payload(self, limit: int) -> list[dict[str, Any]]:
        return self._dispatcher_history_payload(None, None, False, limit)["events"]

    def _operation_payload(self, operation_id: str) -> dict[str, Any]:
        # Keep the existing resource shape; tools return the full operation status.
        return _mcp_payload(self._api.get_operation_response(operation_id))["operation"]

    def _operation_events_payload(self, operation_id: str) -> list[dict[str, Any]]:
        return _mcp_payload(self._api.get_operation_events_response(operation_id))

    def resume_dispatcher_runtime(self) -> dict[str, Any]:
        return _mcp_payload(self._api.resume_operation_dispatcher_response())

    def pause_dispatcher_runtime(self) -> dict[str, Any]:
        return _mcp_payload(self._api.pause_operation_dispatcher_response())


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
        runtime_controller: OperationDispatcherRuntimeController | None = None,
        **fastmcp_kwargs: Any,
    ) -> None:
        self._utility = OperationDispatcherMCPUtility(
            operation_dispatcher, runtime_controller
        )
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
        if transport == "stdio":
            if mount_path is not None:
                raise ValueError("mount_path is only supported for HTTP transports")
            self._mcp.run(transport="stdio")
            return
        if transport not in {"sse", "streamable-http", "http"}:
            raise ValueError(f"unsupported transport: {transport}")
        resolved_transport = "http" if transport == "streamable-http" else transport
        run_kwargs: dict[str, Any] = {
            "transport": resolved_transport,
            "host": self._host,
            "port": self._port,
            "json_response": self._json_response,
        }
        if mount_path is not None:
            run_kwargs["path"] = mount_path
        self._mcp.run(**run_kwargs)

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
    runtime_controller: OperationDispatcherRuntimeController | None = None,
) -> OperationDispatcherMCPUtility:
    """Build MCP utility helpers around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPUtility(operation_dispatcher, runtime_controller)


def create_operation_dispatcher_mcp_server(
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
