from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    Operation,
)
from operation_dispatcher import SimulatedOperationRunner


class DemoDispatcherService:
    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self.host = host

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
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

    def _on_request(self, event: DispatchEvent) -> bool | None:
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

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            f"Received notification event {event.event_type} for operation_id {event.operation_id}"
        )
        self._visualizer.on_notification(event)

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

    async def run_demo(self) -> None:
        self.operation_dispatcher.add(
            Operation(
                payload={
                    "name": "my_operation_1",
                    "task": "pickup",
                    "run_seconds": 10.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        self.operation_dispatcher.add(
            Operation(
                payload={
                    "name": "my_operation_2",
                    "task": "dropoff",
                    "run_seconds": 8.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        app = self.create_app()
        app.run(host=self.host, port=8000, debug=False, use_reloader=False)

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self._visualizer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    demo_dispatcher = DemoDispatcherService(host="0.0.0.0", logger=logger)
    try:
        await demo_dispatcher.run_demo()
    finally:
        demo_dispatcher.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
