from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from pathlib import Path
from uuid import UUID

from flask import Flask
from flasgger import Swagger

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    Operation,
    OperationDispatcher,
    OperationDispatcherMCPServer,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
)


class DemoOperation(Operation):
    name: str
    run_seconds: float = 4.0


class MyDispatcherService:
    """
    Example service demonstrating a shared `OperationDispatcher` with both an MCP interface and a Flask REST API interface.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._completion_timers: dict[UUID, threading.Timer] = {}
        self._runtime_thread: threading.Thread | None = None

        self.operation_dispatcher = OperationDispatcher(
            resource_id="demo_resource_1",
            operation_model=DemoOperation,
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )

    def seed_operations(self) -> None:
        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=DemoOperation(name="pickup", run_seconds=3.0),
                resource_id="robot-1",
                priority=10,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=DemoOperation(name="dropoff", run_seconds=5.0),
                resource_id="robot-1",
                priority=5,
            )
        )

    def create_flask_app(self) -> Flask:
        app = Flask(__name__)
        operation_dispatcher_api = OperationDispatcherOpenAPI(
            self.operation_dispatcher,
            default_operation_name="demo_operation",
        )

        Swagger(
            app,
            template={
                "swagger": "2.0",
                "info": {
                    "title": "Shared Operation Dispatcher API",
                    "version": "1.0.0",
                    "description": "Flask API backed by the same OperationDispatcher used by the MCP server.",
                },
                "definitions": operation_dispatcher_api.get_openapi_definitions(),
            },
        )
        operation_dispatcher_api.register_default_endpoints(app)
        return app

    def create_mcp_server(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> OperationDispatcherMCPServer:
        return OperationDispatcherMCPServer(
            self.operation_dispatcher,
            name="Shared Operation Dispatcher MCP",
            instructions=(
                "Expose the shared operation dispatcher state. Additional tools can "
                "be registered on this server before startup."
            ),
            host=host,
            port=port,
            json_response=True,
        )

    def _on_request(self, event: DispatchEvent) -> bool | None:
        """Request callback"""
        return True

    def _on_notification(self, event: DispatchEvent) -> None:
        """Notification callback"""
        pass

    def _schedule_completion(self, operation_id: UUID) -> None:
        current_operation = self.operation_dispatcher.current_operation
        if current_operation is None or current_operation.id != operation_id:
            return

        delay_seconds = max(0.1, float(current_operation.run_seconds))
        timer = threading.Timer(
            delay_seconds,
            self._complete_if_current,
            args=(operation_id,),
        )
        timer.daemon = True

        previous_timer = self._completion_timers.pop(operation_id, None)
        if previous_timer is not None:
            previous_timer.cancel()

        self._completion_timers[operation_id] = timer
        timer.start()

    def _complete_if_current(self, operation_id: UUID) -> None:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is None or current.operation.id != operation_id:
            return

        self.operation_dispatcher.complete_current()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    service = MyDispatcherService()
    service.seed_operations()
    service.start_dispatcher_runtime()

    flask_app = service.create_flask_app()
    flask_thread = threading.Thread(
        target=flask_app.run,
        kwargs={
            "host": "127.0.0.1",
            "port": 5000,
            "debug": False,
            "use_reloader": False,
        },
        name="DualInterfaceFlaskServer",
        daemon=True,
    )
    flask_thread.start()

    print("Flask API available at http://127.0.0.1:5000")
    print("Flask OpenAPI docs available at http://127.0.0.1:5000/apidocs/")
    print("MCP SSE endpoint available at http://127.0.0.1:8000/sse")
    print("MCP message endpoint available at http://127.0.0.1:8000/messages/")

    mcp_server = service.create_mcp_server(host="0.0.0.0", port=8000)
    try:
        mcp_server.run(transport="sse")
    finally:
        service.stop()


if __name__ == "__main__":
    main()
