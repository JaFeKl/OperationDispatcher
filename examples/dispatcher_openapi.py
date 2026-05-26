from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
)
from operation_dispatcher import SimulatedOperationRunner


class DemoDispatcherService:
    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request_handler,
            on_notification_callback=self._on_notification_handler,
            start_request_max_retries=3,
            start_request_retry_cooldown_seconds=1.0,
            request_event_timeout_seconds=2.0,
            logger=logger,
        )

        self._visualizer = BrowserEventVisualizer(
            host=host,
            port=8765,
            operation_dispatcher=self.operation_dispatcher,
        )
        self._visualizer.start()
        logger.info("Event visualizer available at http://{}:8765".format(host))

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=logger,
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
        """Request callback for all dispatcher events."""
        self._visualizer.on_request(event)
        self._logger.info(
            f"Received request {event.event_type} for operation_id {event.operation_id}"
        )

        scheduled_operation = self.operation_dispatcher.get_scheduled_operation(
            event.operation_id
        )
        if scheduled_operation is None:
            self._logger.warning(
                f"received request event for unknown operation_id {event.operation_id}"
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
            except RuntimeError as e:
                self._logger.warning(
                    f"start request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            try:
                self._simulated_runner.cancel(operation_id=scheduled_operation.id)
                return True
            except RuntimeError as e:
                self._logger.warning(
                    f"cancel request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            try:
                self._simulated_runner.pause(operation_id=scheduled_operation.id)
                return True
            except RuntimeError as e:
                self._logger.warning(
                    f"pause request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            try:
                self._simulated_runner.resume(operation_id=scheduled_operation.id)
            except RuntimeError as e:
                self._logger.warning(
                    f"resume request failed for operation {event.operation_id} with error: {e}"
                )
                return False
            return True

        return None

    def _on_notification_handler(self, event: DispatchEvent) -> None:
        """
        Notification callback for all dispatcher events.
        In a real implementation, this could be used to trigger side effects in
        response to state changes in the dispatcher, e.g. updating a dashboard or triggering other actions in the system.
        """
        self._visualizer.on_notification(event)
        self._logger.info(
            f"Received event {event.event_type} for operation_id {event.operation_id}"
        )

    def _on_completed(self, operation_id: UUID) -> None:
        """Callback for simulated operation completion. This should be called when an operation is completed by the simulated runner."""
        current_operation = self.operation_dispatcher.current_scheduled_operation
        if current_operation is None or current_operation.id != operation_id:
            self._logger.warning(
                f"simulated completion callback received for non-current operation_id {operation_id}"
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

        @app.get("/")
        def root() -> tuple:
            return (
                jsonify(
                    {
                        "message": "Welcome to the Operation Dispatcher Demo API! Visit /docs/ for API documentation."
                    }
                ),
                200,
            )

        operation_dispatcher_api.register_default_endpoints(app)

        return app

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self._visualizer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    host = "0.0.0.0"

    demo_dispatcher = DemoDispatcherService(host=host, logger=logger)
    demo_dispatcher.add_demo_operations()

    app = demo_dispatcher.create_app()
    try:
        app.run(host=host, port=8000, debug=False, use_reloader=False)
    finally:
        demo_dispatcher.shutdown()
