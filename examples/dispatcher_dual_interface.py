from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from uuid import UUID

from flask import Flask
from flasgger import Swagger

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherMCPServer,
    OperationDispatcherOpenAPI,
    Operation,
    SimulatedOperationRunner,
)
from operation_dispatcher.operation_dispatcher_mcp import (
    DispatcherMCPPrompts,
    DispatcherMCPResources,
    DispatcherMCPTools,
)
from operation_dispatcher.runtime_controller import OperationDispatcherRuntimeController


class DemoDispatcherService:
    """
    Demonstrates a shared OperationDispatcher exposed through both:

    - OpenAPI (Flask)
    - MCP

    Both interfaces share the same dispatcher instance.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        flask_port: int = 8000,
        mcp_port: int = 8001,
        visualizer_port: int = 8765,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self._host = host
        self._flask_port = flask_port
        self._mcp_port = mcp_port
        self._visualizer_port = visualizer_port

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            start_paused=True,
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )
        self._runtime_controller = OperationDispatcherRuntimeController(
            self.operation_dispatcher
        )
        self.operation_dispatcher_api = OperationDispatcherOpenAPI(
            self.operation_dispatcher
        )

        self.visualizer = BrowserEventVisualizer(
            host=self._host,
            port=self._visualizer_port,
            operation_dispatcher=self.operation_dispatcher,
        )
        self.visualizer.start()

        self._logger.info(
            "Event visualizer available at http://%s:%s",
            self._host,
            self._visualizer_port,
        )

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=self._logger,
        )

    # -------------------------------------------------------------------------
    # Dispatcher callbacks
    # -------------------------------------------------------------------------

    def _on_request(self, event: DispatchEvent) -> bool | None:
        """
        Handle dispatcher lifecycle requests.
        """
        self.visualizer.on_request(event)
        self._logger.info(
            f"Received request {event.event_type} for operation_id {event.operation_id}"
        )
        if event.operation_id is None:
            return None
        operation = self.operation_dispatcher.get_operation(event.operation_id)
        if operation is None:
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            run_seconds = float(operation.payload.get("run_seconds", 1.0))
            self._simulated_runner.start(
                operation_id=operation.id,
                run_seconds=run_seconds,
            )
            print("Started operation with payload:", operation.payload)
            return True
        return None

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            f"Received notification event {event.event_type} for operation_id {event.operation_id}"
        )

        self.visualizer.on_notification(event)

    def _on_completed(self, operation_id: UUID) -> None:
        """
        Completion callback from the simulated runner.
        """
        self.operation_dispatcher.complete_operation(operation_id)

    # -------------------------------------------------------------------------
    # OpenAPI
    # -------------------------------------------------------------------------

    def create_flask_app(self) -> Flask:
        app = Flask(__name__, static_url_path="/app_static")

        swagger_template = {
            "swagger": "2.0",
            "info": {
                "title": "Operation Dispatcher API",
                "version": "1.0.0",
                "description": (
                    "Example API exposing an OperationDispatcher instance."
                ),
            },
            "definitions": self.operation_dispatcher_api.get_openapi_definitions(),
        }

        swagger_config = {
            "headers": [],
            "specs": [
                {
                    "endpoint": "openapi",
                    "route": "/openapi.json",
                    "rule_filter": lambda rule: True,
                    "model_filter": lambda tag: True,
                }
            ],
            "swagger_ui": True,
            "specs_route": "/",
            "static_url_path": "/flasgger_static",
        }

        Swagger(app, template=swagger_template, config=swagger_config)

        self.operation_dispatcher_api.register_default_endpoints(app)

        return app

    # -------------------------------------------------------------------------
    # MCP
    # -------------------------------------------------------------------------

    def create_mcp_server(self) -> OperationDispatcherMCPServer:
        return OperationDispatcherMCPServer(
            self.operation_dispatcher,
            name="Shared Operation Dispatcher",
            instructions=(
                "This MCP server exposes a shared OperationDispatcher. "
                "Use the available tools to inspect scheduling state, "
                "dispatch operations, control lifecycle events, and "
                "analyze execution progress."
            ),
            host=self._host,
            port=self._mcp_port,
            tools=list(DispatcherMCPTools),
            resources=list(DispatcherMCPResources),
            prompts=list(DispatcherMCPPrompts),
            json_response=True,
        )

    # -------------------------------------------------------------------------
    # Demo data
    # -------------------------------------------------------------------------

    def load_demo_schedule(self) -> None:
        operations = [
            Operation(
                payload={
                    "name": "pickup_part",
                    "task": "pickup",
                    "run_seconds": 10.0,
                },
                resource_id="robot-1",
                priority=0,
            ),
            Operation(
                payload={
                    "name": "deliver_part",
                    "task": "dropoff",
                    "run_seconds": 8.0,
                },
                resource_id="robot-1",
                priority=0,
            ),
        ]

        for operation in operations:
            self.operation_dispatcher.add_operation(operation)

    # -------------------------------------------------------------------------
    # Runtime
    # -------------------------------------------------------------------------

    async def run(self) -> None:
        self.load_demo_schedule()

        self._runtime_controller.start()
        flask_app = self.create_flask_app()

        flask_thread = threading.Thread(
            target=flask_app.run,
            kwargs={
                "host": self._host,
                "port": self._flask_port,
                "debug": False,
                "use_reloader": False,
            },
            daemon=True,
            name="FlaskServer",
        )

        flask_thread.start()

        self._logger.info(
            "OpenAPI server available at http://%s:%s",
            self._host,
            self._flask_port,
        )

        mcp_server = self.create_mcp_server()

        #
        # MCP blocks the current thread.
        #
        await asyncio.to_thread(
            mcp_server.run,
            transport="sse",
        )

    def shutdown(self) -> None:
        self._logger.info("Shutting down demo service")
        self._simulated_runner.cancel()
        self._runtime_controller.stop()
        self.visualizer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    service = DemoDispatcherService()

    try:
        await service.run()
    finally:
        service.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
