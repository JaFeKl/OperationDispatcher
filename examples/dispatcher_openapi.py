from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import Field

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    Operation,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
)
from operation_dispatcher import SimulatedOperationRunner


class DemoOperation(Operation):
    """User defined operation type for demonstration purposes."""

    name: str
    source_station: str
    target_station: str
    pallet_id: str
    retries: int = 0
    run_seconds: float = Field(default=10.0, gt=0)


class DemoDispatcherService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=logger,
        )

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            operation_model=DemoOperation,
            on_request_callback=self._on_request_handler,
            on_notification_callback=self._on_notification_handler,
            start_request_max_retries=3,
            start_request_retry_cooldown_seconds=1.0,
            request_event_timeout_seconds=2.0,
            logger=logger,
        )

        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=DemoOperation(
                    name="pickup_pallet",
                    source_station="INBOUND_A",
                    target_station="BUFFER_01",
                    pallet_id="PALLET-1001",
                    run_seconds=10.0,
                ),
                resource_id="robot-1",
                priority=0,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=DemoOperation(
                    name="dropoff_pallet",
                    source_station="BUFFER_01",
                    target_station="OUTBOUND_B",
                    pallet_id="PALLET-1001",
                    run_seconds=10.0,
                ),
                resource_id="robot-1",
                priority=0,
            )
        )

    def _on_request_handler(self, event: DispatchEvent) -> bool | None:
        """Request callback for all dispatcher events."""

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
                    operation_id=scheduled_operation.operation.id,
                    run_seconds=scheduled_operation.operation.run_seconds,
                )
                return True
            except RuntimeError as e:
                self._logger.warning(
                    f"start request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            try:
                self._simulated_runner.cancel(
                    operation_id=scheduled_operation.operation.id
                )
                return True
            except RuntimeError as e:
                self._logger.warning(
                    f"cancel request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            try:
                self._simulated_runner.pause(
                    operation_id=scheduled_operation.operation.id
                )
                return True
            except RuntimeError as e:
                self._logger.warning(
                    f"pause request failed for operation {event.operation_id} with error: {e}"
                )
                return False

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            try:
                self._simulated_runner.resume(
                    operation_id=scheduled_operation.operation.id
                )
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
        self._logger.info(
            f"Received event {event.event_type} for operation_id {event.operation_id}"
        )

    def _on_completed(self, operation_id: UUID) -> None:
        """Callback for simulated operation completion. This should be called when an operation is completed by the simulated runner."""
        current_operation = self.operation_dispatcher.current_scheduled_operation
        if current_operation is None or current_operation.operation.id != operation_id:
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
            "specs_route": "/docs/",
            "static_url_path": "/flasgger_static",
        }
        Swagger(app, template=swagger_template, config=swagger_config)

        @app.get("/")
        def root() -> tuple:
            return (
                jsonify(
                    {
                        "message": "Operation Dispatcher Flask + OpenAPI demo",
                        "notes": {
                            "start_handshake": "first start request for each operation is denied once to demonstrate retry/cooldown",
                            "completion": "operations are auto-completed after operation.payload.run_seconds (default 5.0s)",
                            "simulation": "a dedicated simulated worker thread supports true pause/resume for /operation_dispatcher/current_operation/pause and /operation_dispatcher/current_operation/resume",
                        },
                        "docs": "/docs/",
                        "openapi": "/openapi.json",
                        "queue": "/operation_dispatcher/queue",
                        "history": "/operation_dispatcher/history",
                        "get_current_operation": "/operation_dispatcher/current_operation",
                        "get_next_operation": "/operation_dispatcher/next_operation",
                        "add_operation": "/operation_dispatcher/add",
                        "cancel_operation": "/operation_dispatcher/cancel_operation",
                        "cancel_current_operation": "/operation_dispatcher/current_operation/cancel",
                        "pause_current_operation": "/operation_dispatcher/current_operation/pause",
                        "resume_current_operation": "/operation_dispatcher/current_operation/resume",
                        "get_dispatcher_state": "/operation_dispatcher/state",
                        "start": "/operation_dispatcher/start",
                        "stop": "/operation_dispatcher/stop",
                        "resume": "/operation_dispatcher/resume",
                    }
                ),
                200,
            )

        operation_dispatcher_api.register_default_endpoints(app)

        return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    demo_dispatcher = DemoDispatcherService(logger=logger)
    app = demo_dispatcher.create_app()
    logger.info("Starting Flask demo on http://localhost:8000")
    logger.info("Open docs at http://localhost:8000/docs/")
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
