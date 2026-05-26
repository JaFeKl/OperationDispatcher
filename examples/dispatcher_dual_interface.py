from __future__ import annotations

import logging
import threading
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherMCPServer,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
    SimulatedOperationRunner,
)


class DemoDispatcherDualInterfaceService:
    """
    Example service demonstrating a shared `OperationDispatcher` with both
    OpenAPI (Flask) and MCP interfaces.

    Runtime ownership is centralized in `OperationDispatcherOpenAPI`. MCP receives
    the same OpenAPI runtime owner instance and exposes lifecycle tools
    automatically.
    """

    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request_handler,
            on_notification_callback=self._on_notification_handler,
            start_request_max_retries=3,
            start_request_retry_cooldown_seconds=1.0,
            request_event_timeout_seconds=2.0,
            logger=self._logger,
        )

        self._visualizer = BrowserEventVisualizer(
            host=host,
            port=8765,
            operation_dispatcher=self.operation_dispatcher,
        )
        self._visualizer.start()

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=logger,
        )

        self.operation_dispatcher_api = OperationDispatcherOpenAPI(
            self.operation_dispatcher,
        )

    def add_demo_operations(self) -> None:
        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={
                    "name": "pickup_pallet",
                    "source_station": "INBOUND_A",
                    "target_station": "BUFFER_01",
                    "pallet_id": "PALLET-1001",
                    "run_seconds": 10.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={
                    "name": "dropoff_pallet",
                    "source_station": "BUFFER_01",
                    "target_station": "OUTBOUND_B",
                    "pallet_id": "PALLET-1001",
                    "run_seconds": 10.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )

    def _on_request_handler(self, event: DispatchEvent) -> bool | None:
        self._visualizer.on_request(event)

        scheduled_operation = self.operation_dispatcher.get_scheduled_operation(
            event.operation_id
        )
        if scheduled_operation is None:
            self._logger.warning(
                "received request event for unknown operation_id %s",
                event.operation_id,
            )
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            try:
                self._simulated_runner.start(
                    operation_id=scheduled_operation.id,
                    run_seconds=float(
                        scheduled_operation.payload.get("run_seconds", 5.0)
                    ),
                )
                return True
            except RuntimeError as error:
                self._logger.warning(
                    "start request failed for operation %s with error: %s",
                    event.operation_id,
                    error,
                )
                return False

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            try:
                self._simulated_runner.cancel(operation_id=scheduled_operation.id)
                return True
            except RuntimeError as error:
                self._logger.warning(
                    "cancel request failed for operation %s with error: %s",
                    event.operation_id,
                    error,
                )
                return False

        if event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            try:
                self._simulated_runner.pause(operation_id=scheduled_operation.id)
                return True
            except RuntimeError as error:
                self._logger.warning(
                    "pause request failed for operation %s with error: %s",
                    event.operation_id,
                    error,
                )
                return False

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            try:
                self._simulated_runner.resume(operation_id=scheduled_operation.id)
                return True
            except RuntimeError as error:
                self._logger.warning(
                    "resume request failed for operation %s with error: %s",
                    event.operation_id,
                    error,
                )
                return False

        return None

    def _on_notification_handler(self, event: DispatchEvent) -> None:
        self._visualizer.on_notification(event)
        self._logger.info(
            "Received event %s for operation_id %s",
            event.event_type,
            event.operation_id,
        )

    def _on_completed(self, operation_id: UUID) -> None:
        current_operation = self.operation_dispatcher.current_scheduled_operation
        if current_operation is None or current_operation.id != operation_id:
            self._logger.warning(
                "simulated completion callback received for non-current operation_id %s",
                operation_id,
            )
            return

        self.operation_dispatcher.complete_current()

    def create_app(self) -> Flask:
        app = Flask(__name__, static_url_path="/app_static")

        operation_dispatcher_api = OperationDispatcherOpenAPI(
            self.operation_dispatcher,
        )

        swagger_template = {
            "swagger": "2.0",
            "info": {
                "title": "Operation Dispatcher API",
                "version": "1.0.0",
                "description": "Example Flask API exposing operation dispatcher endpoints.",
            },
            "definitions": operation_dispatcher_api.get_openapi_definitions(),
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
        operation_dispatcher_api.register_default_endpoints(app)
        return app

    def create_mcp_server(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> OperationDispatcherMCPServer:
        return OperationDispatcherMCPServer(
            self.operation_dispatcher_api,
            name="Shared Operation Dispatcher MCP",
            instructions=(
                "Expose and control the shared operation dispatcher state. "
                "Lifecycle is delegated to the shared OpenAPI runtime controller."
            ),
            host=host,
            port=port,
            json_response=True,
        )

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self._visualizer.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    host = "0.0.0.0"
    flask_port = 8000
    mcp_port = 8001

    service = DemoDispatcherDualInterfaceService(host=host, logger=logger)
    service.add_demo_operations()

    app = service.create_app()

    # run flask app in its own thread.
    flask_thread = threading.Thread(
        target=app.run,
        kwargs={
            "host": host,
            "port": flask_port,
            "debug": False,
            "use_reloader": False,
        },
        name="DualInterfaceFlaskServer",
        daemon=True,
    )
    flask_thread.start()

    # print("Flask API available at http://127.0.0.1:5000")
    # print("Flask OpenAPI docs available at http://127.0.0.1:5000/")
    # print("MCP SSE endpoint available at http://127.0.0.1:8000/sse")
    # print("MCP message endpoint available at http://127.0.0.1:8000/messages/")
    # print("Event visualizer available at http://127.0.0.1:8765")
    # print("Runtime owner: OperationDispatcherOpenAPI")
    # print("MCP uses built-in lifecycle tools delegated to shared OpenAPI runtime owner")

    mcp_server = service.create_mcp_server(host=host, port=mcp_port)
    try:
        mcp_server.run(transport="sse")
    finally:
        service.shutdown()
        flask_thread.join(timeout=5.0)


if __name__ == "__main__":
    main()
