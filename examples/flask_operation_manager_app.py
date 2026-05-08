from __future__ import annotations

import logging
import threading
import time
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel

from operation_manager import (
    Operation,
    OperationManager,
    OperationManagerEventType,
    OperationManagerOpenAPI,
)
from operation_manager.models import OperationManagerEvent


class ExampleOperationPayload(BaseModel):
    source_station: str
    target_station: str
    pallet_id: str
    retries: int = 0


class DemoAgentService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger
        self._running_timers: dict[UUID, threading.Timer] = {}
        self._start_attempts: dict[UUID, int] = {}

        self.operation_manager = OperationManager(
            agent_id="agent-1",
            on_request_callback=self._on_request_handler,
            on_notification_callback=self._on_notification_handler,
            start_request_max_retries=3,
            start_request_retry_cooldown_seconds=1.0,
            request_event_timeout_seconds=2.0,
            payload_model=ExampleOperationPayload,
            logger=logger,
        )

        self.operation_manager.add(
            Operation(
                name="pickup_pallet",
                agent_id="agent-1",
                priority=10,
                payload=ExampleOperationPayload(
                    source_station="INBOUND_A",
                    target_station="BUFFER_01",
                    pallet_id="PALLET-1001",
                ).model_dump(),
            )
        )
        self.operation_manager.add(
            Operation(
                name="dropoff_pallet",
                agent_id="agent-1",
                priority=5,
                payload=ExampleOperationPayload(
                    source_station="BUFFER_01",
                    target_station="OUTBOUND_B",
                    pallet_id="PALLET-1001",
                ).model_dump(),
            )
        )

    def _on_request_handler(self, event: OperationManagerEvent) -> bool | None:
        if event.event_type is OperationManagerEventType.OPERATION_START_REQUESTED:
            return self._allow_start_with_one_initial_denial(event)

        if (
            event.event_type
            is OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED
        ):
            return True

        if event.event_type is OperationManagerEventType.OPERATION_CANCEL_REQUESTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._cancel_running_timer(operation_id)

        if event.event_type is OperationManagerEventType.OPERATION_STOP_REQUESTED:
            current = self.operation_manager.current_operation
            if current is not None:
                self._cancel_running_timer(current.id)

        if event.event_type is OperationManagerEventType.OPERATION_RESUME_REQUESTED:
            return True

        return None

    def _on_notification_handler(self, event: OperationManagerEvent) -> None:
        if event.event_type is OperationManagerEventType.OPERATION_STARTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._schedule_completion(operation_id, delay_seconds=1.0)

        if event.event_type is OperationManagerEventType.OPERATION_START_DENIED:
            if self._logger is not None:
                self._logger.info(
                    "start denied for operation %s with metadata=%s",
                    event.operation_id,
                    event.data,
                )

        if event.event_type is OperationManagerEventType.OPERATION_CANCELLED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._cancel_running_timer(operation_id)

    def _allow_start_with_one_initial_denial(
        self, event: OperationManagerEvent
    ) -> bool:
        operation_id = event.operation_id
        if operation_id is None:
            return False

        attempt = self._start_attempts.get(operation_id, 0) + 1
        self._start_attempts[operation_id] = attempt

        if attempt == 1:
            if self._logger is not None:
                self._logger.info(
                    "denying first start request for operation %s to demonstrate retry/cooldown",
                    operation_id,
                )
            return False

        return True

    def _schedule_completion(self, operation_id: UUID, delay_seconds: float) -> None:
        self._cancel_running_timer(operation_id)

        def complete_if_current() -> None:
            current = self.operation_manager.current_operation
            if current is not None and current.id == operation_id:
                self.operation_manager.complete_current()

        timer = threading.Timer(delay_seconds, complete_if_current)
        self._running_timers[operation_id] = timer
        timer.start()

    def _cancel_running_timer(self, operation_id: UUID) -> None:
        timer = self._running_timers.pop(operation_id, None)
        if timer is not None:
            timer.cancel()

    def create_app(self) -> Flask:
        app = Flask(__name__, static_url_path="/app_static")

        operation_manager_api = OperationManagerOpenAPI(
            self.operation_manager,
        )

        swagger_template = {
            "swagger": "2.0",
            "info": {
                "title": "Operation Manager API",
                "version": "1.0.0",
                "description": "Example Flask API exposing operation manager endpoints.",
            },
            "definitions": operation_manager_api.get_openapi_definitions(),
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
                        "message": "Operation Manager Flask + OpenAPI demo",
                        "notes": {
                            "start_handshake": "first start request for each operation is denied once to demonstrate retry/cooldown",
                            "completion": "operations are auto-completed 1 second after OPERATION_STARTED",
                        },
                        "docs": "/docs/",
                        "openapi": "/openapi.json",
                        "schedule": "/operation_manager/schedule",
                        "history": "/operation_manager/history",
                        "get_current_operation": "/operation_manager/current_operation",
                        "get_next_operation": "/operation_manager/next_operation",
                        "add_operation": "/operation_manager/add_operation",
                        "cancel_operation": "/operation_manager/cancel_operation",
                        "get_operation_manager_state": "/operation_manager/state",
                        "start": "/operation_manager/start",
                        "stop": "/operation_manager/stop",
                        "pause": "/operation_manager/pause",
                        "resume": "/operation_manager/resume",
                    }
                ),
                200,
            )

        operation_manager_api.register_default_endpoints(app)

        return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    demo_agent = DemoAgentService(logger=logger)
    app = demo_agent.create_app()
    logger.info("starting Flask demo on http://localhost:8000")
    logger.info("open docs at http://localhost:8000/docs/")
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
