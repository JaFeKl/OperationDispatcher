from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel, Field

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
    run_seconds: float = Field(default=5.0, gt=0)


class SimulatedOperationRunner:
    def __init__(
        self,
        on_complete: Callable[[UUID], None],
        logger: logging.Logger | None = None,
        tick_seconds: float = 0.05,
    ) -> None:
        self._on_complete = on_complete
        self._logger = logger
        self._tick_seconds = tick_seconds

        self._lock = threading.Lock()
        self._operation_id: UUID | None = None
        self._remaining_seconds = 0.0
        self._worker_thread: threading.Thread | None = None
        self._pause_event = threading.Event()
        self._stop_event = threading.Event()

    def start(self, operation_id: UUID, run_seconds: float) -> None:
        if run_seconds <= 0:
            raise ValueError("run_seconds must be greater than 0")

        self.cancel()

        with self._lock:
            self._operation_id = operation_id
            self._remaining_seconds = run_seconds
            self._stop_event.clear()
            self._pause_event.set()
            self._worker_thread = threading.Thread(
                target=self._run_worker,
                name=f"SimulatedOperationRunner-{operation_id}",
                daemon=True,
            )
            worker_thread = self._worker_thread

        worker_thread.start()

    def pause(self, operation_id: UUID) -> bool:
        with self._lock:
            if self._operation_id != operation_id or self._worker_thread is None:
                return False
            self._pause_event.clear()
            return True

    def resume(self, operation_id: UUID) -> bool:
        with self._lock:
            if self._operation_id != operation_id or self._worker_thread is None:
                return False
            self._pause_event.set()
            return True

    def cancel(self, operation_id: UUID | None = None) -> bool:
        with self._lock:
            if (
                operation_id is not None
                and self._operation_id is not None
                and operation_id != self._operation_id
            ):
                return False

            worker_thread = self._worker_thread
            self._stop_event.set()
            self._pause_event.set()
            self._operation_id = None
            self._remaining_seconds = 0.0
            self._worker_thread = None

        if (
            worker_thread is not None
            and worker_thread is not threading.current_thread()
            and worker_thread.is_alive()
        ):
            worker_thread.join(timeout=0.2)

        self._stop_event.clear()
        return True

    def _run_worker(self) -> None:
        last_tick = time.monotonic()

        while True:
            if self._stop_event.is_set():
                return

            if not self._pause_event.wait(timeout=self._tick_seconds):
                last_tick = time.monotonic()
                continue

            now = time.monotonic()
            elapsed_seconds = max(0.0, now - last_tick)
            last_tick = now

            with self._lock:
                if self._operation_id is None:
                    return

                self._remaining_seconds -= elapsed_seconds
                if self._remaining_seconds > 0:
                    continue

                operation_id = self._operation_id
                self._operation_id = None
                self._remaining_seconds = 0.0
                self._worker_thread = None

            try:
                self._on_complete(operation_id)
            except Exception:
                if self._logger is not None:
                    self._logger.exception(
                        "simulated operation completion callback failed"
                    )
            return


class DemoAgentService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger
        self._start_attempts: dict[UUID, int] = {}
        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._complete_operation_if_current,
            logger=logger,
        )

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
                    run_seconds=8.0,
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
                    run_seconds=6.0,
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
                self._simulated_runner.cancel(operation_id)
                return True

        if event.event_type is OperationManagerEventType.OPERATION_STOP_REQUESTED:
            current = self.operation_manager.current_operation
            if current is not None:
                self._simulated_runner.pause(current.id)
                return False

        if event.event_type is OperationManagerEventType.OPERATION_RESUME_REQUESTED:
            current = self.operation_manager.current_operation
            if current is not None:
                self._simulated_runner.resume(current.id)
            return True

        return None

    def _on_notification_handler(self, event: OperationManagerEvent) -> None:
        if event.event_type is OperationManagerEventType.OPERATION_STARTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulate_operation_run(operation_id)

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
                self._simulated_runner.cancel(operation_id)

        if event.event_type in {
            OperationManagerEventType.OPERATION_COMPLETED,
            OperationManagerEventType.OPERATION_FAILED,
            OperationManagerEventType.OPERATION_STOPPED,
        }:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulated_runner.cancel(operation_id)

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

    def _simulate_operation_run(self, operation_id: UUID) -> None:
        delay_seconds = self._resolve_simulated_run_seconds(operation_id)
        self._simulated_runner.start(operation_id, delay_seconds)

    def _complete_operation_if_current(self, operation_id: UUID) -> None:
        current = self.operation_manager.current_operation
        if current is not None and current.id == operation_id:
            self.operation_manager.complete_current()

    def _resolve_simulated_run_seconds(self, operation_id: UUID) -> float:
        current = self.operation_manager.current_operation
        if current is None or current.id != operation_id:
            return 5.0

        payload = current.payload
        run_seconds = payload.get("run_seconds")
        if isinstance(run_seconds, (int, float)) and run_seconds > 0:
            return float(run_seconds)

        return 5.0

    def create_app(self) -> Flask:
        app = Flask(
            __name__,
            static_url_path="/app_static",
        )

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
                            "completion": "operations are auto-completed after payload.run_seconds (default 5.0s)",
                            "simulation": "a dedicated simulated worker thread supports true pause/resume for stop_current_operation and resume_current_operation",
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
