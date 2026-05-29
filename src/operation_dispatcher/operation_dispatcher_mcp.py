from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP
from operation_dispatcher.operation_dispatcher import OperationDispatcher


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
        manage_dispatcher_runtime: bool = True,
        runtime_startup_timeout_seconds: float = 1.0,
        runtime_stop_join_timeout_seconds: float = 2.0,
        **fastmcp_kwargs: Any,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._manage_dispatcher_runtime = manage_dispatcher_runtime
        self._runtime_startup_timeout_seconds = runtime_startup_timeout_seconds
        self._runtime_stop_join_timeout_seconds = runtime_stop_join_timeout_seconds
        self._runtime_thread: threading.Thread | None = None
        self._runtime_last_error: str | None = None
        self._runtime_lock = threading.Lock()

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
        state = self._operation_dispatcher.get_state().model_dump(mode="json")
        state["runtime_thread_alive"] = (
            self._runtime_thread.is_alive()
            if self._runtime_thread is not None
            else False
        )
        state["runtime_last_error"] = self._runtime_last_error
        state["runtime_managed_by"] = (
            "mcp" if self._manage_dispatcher_runtime else "external"
        )
        return state

    def start_dispatcher_runtime(self) -> dict[str, Any]:
        if not self._manage_dispatcher_runtime:
            return {
                "message": "dispatcher runtime is managed externally",
                "state": self._dispatcher_state_payload(),
            }

        with self._runtime_lock:
            if self._operation_dispatcher.is_running:
                return {
                    "message": "operation dispatcher is already running",
                    "state": self._dispatcher_state_payload(),
                }

            runtime_thread = self._runtime_thread
            if runtime_thread is not None and runtime_thread.is_alive():
                return {
                    "message": "operation dispatcher runtime thread already active",
                    "state": self._dispatcher_state_payload(),
                }

            self._runtime_last_error = None

            def run_dispatcher() -> None:
                try:
                    asyncio.run(self._operation_dispatcher.run())
                except Exception as error:
                    self._runtime_last_error = str(error)

            self._runtime_thread = threading.Thread(
                target=run_dispatcher,
                name="OperationDispatcherMCPRuntimeThread",
                daemon=True,
            )
            self._runtime_thread.start()

        deadline = time.time() + self._runtime_startup_timeout_seconds
        while not self._operation_dispatcher.is_running and time.time() < deadline:
            time.sleep(0.01)

        state = self._dispatcher_state_payload()
        if self._operation_dispatcher.is_running:
            return {
                "message": "operation dispatcher started",
                "state": state,
            }

        if self._runtime_last_error:
            return {
                "message": "operation dispatcher failed to start",
                "state": state,
                "error": self._runtime_last_error,
            }

        return {
            "message": "operation dispatcher start requested",
            "state": state,
        }

    def stop_dispatcher_runtime(self) -> dict[str, Any]:
        if not self._manage_dispatcher_runtime:
            return {
                "message": "dispatcher runtime is managed externally",
                "state": self._dispatcher_state_payload(),
            }

        with self._runtime_lock:
            runtime_thread = self._runtime_thread
            runtime_active = runtime_thread is not None and runtime_thread.is_alive()
            if not self._operation_dispatcher.is_running and not runtime_active:
                return {
                    "message": "operation dispatcher is not running",
                    "state": self._dispatcher_state_payload(),
                }

            self._operation_dispatcher.request_stop()

        if runtime_thread is not None and runtime_thread.is_alive():
            runtime_thread.join(timeout=self._runtime_stop_join_timeout_seconds)

        return {
            "message": "operation dispatcher stopped",
            "state": self._dispatcher_state_payload(),
        }

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
    operation_dispatcher: OperationDispatcher,
    **kwargs: Any,
) -> OperationDispatcherMCPServer:
    """Build an MCP server around an existing `OperationDispatcher` instance."""

    return OperationDispatcherMCPServer(operation_dispatcher, **kwargs)
