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


class DemoDispatcherService:
    """
    Demonstrates a shared OperationDispatcher exposed through both:

    - OpenAPI (Flask)
    - MCP (SSE transport)

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

        #
        # Core dispatcher
        #
        self.dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )

        #
        # Shared runtime owner
        #
        self.dispatcher_api = OperationDispatcherOpenAPI(self.dispatcher)

        #
        # Visualization
        #
        self.visualizer = BrowserEventVisualizer(
            host=self._host,
            port=self._visualizer_port,
            operation_dispatcher=self.dispatcher,
        )
        self.visualizer.start()

        self._logger.info(
            "Event visualizer available at http://%s:%s",
            self._host,
            self._visualizer_port,
        )

        #
        # Simulated execution backend
        #
        self.runner = SimulatedOperationRunner(
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
        self._logger.info(
            "Received request %s for operation %s",
            event.event_type,
            event.operation_id,
        )

        self.visualizer.on_request(event)

        operation = self.dispatcher.get_scheduled_operation(event.operation_id)

        if operation is None:
            self._logger.warning(
                "Unknown operation_id %s",
                event.operation_id,
            )
            return None

        handlers: dict[EventType, Callable[[UUID], None]] = {
            EventType.OPERATION_START_REQUESTED: self.runner.start,
            EventType.OPERATION_CANCEL_REQUESTED: self.runner.cancel,
            EventType.OPERATION_PAUSE_REQUESTED: self.runner.pause,
            EventType.OPERATION_RESUME_REQUESTED: self.runner.resume,
        }

        handler = handlers.get(event.event_type)

        if handler is None:
            return None

        try:
            kwargs = {"operation_id": operation.id}

            if event.event_type is EventType.OPERATION_START_REQUESTED:
                kwargs["run_seconds"] = float(operation.payload.get("run_seconds", 5.0))

            handler(**kwargs)
            return True

        except RuntimeError as error:
            self._logger.warning(
                "Failed to handle %s for operation %s: %s",
                event.event_type,
                event.operation_id,
                error,
            )
            return False

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            "Received notification %s for operation %s",
            event.event_type,
            event.operation_id,
        )

        self.visualizer.on_notification(event)

    def _on_completed(self, operation_id: UUID) -> None:
        """
        Completion callback from the simulated runner.
        """
        current = self.dispatcher.current_scheduled_operation

        if current is None or current.id != operation_id:
            self._logger.warning(
                "Completion received for non-current operation %s",
                operation_id,
            )
            return

        self.dispatcher.complete_current()

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
            "definitions": self.dispatcher_api.get_openapi_definitions(),
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

        self.dispatcher_api.register_default_endpoints(app)

        return app

    # -------------------------------------------------------------------------
    # MCP
    # -------------------------------------------------------------------------

    def create_mcp_server(self) -> OperationDispatcherMCPServer:
        return OperationDispatcherMCPServer(
            self.dispatcher,
            name="Shared Operation Dispatcher",
            instructions=(
                "This MCP server exposes a shared OperationDispatcher. "
                "Use the available tools to inspect scheduling state, "
                "dispatch operations, control lifecycle events, and "
                "analyze execution progress."
            ),
            host=self._host,
            port=self._mcp_port,
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
            self.dispatcher.add(operation)

    # -------------------------------------------------------------------------
    # Runtime
    # -------------------------------------------------------------------------

    async def run(self) -> None:
        self.load_demo_schedule()

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

        self.runner.cancel()
        self.visualizer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    service = DemoDispatcherService()

    try:
        await service.run()
    finally:
        service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
