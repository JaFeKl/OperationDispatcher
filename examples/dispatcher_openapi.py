from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel, Field

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    Operation,
    OperationDispatcher,
    OperationDispatcherOpenAPI,
    ScheduledOperation,
)


class ExampleOperationPayload(BaseModel):
    source_station: str
    target_station: str
    pallet_id: str
    retries: int = 0
    run_seconds: float = Field(default=5.0, gt=0)


class WarehouseOperation(Operation):
    name: str
    payload: ExampleOperationPayload


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


class DemoDispatcherService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger
        self._start_attempts: dict[UUID, int] = {}
        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._complete_operation_if_current,
            logger=logger,
        )

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            operation_model=WarehouseOperation,
            on_request_callback=self._on_request_handler,
            on_notification_callback=self._on_notification_handler,
            start_request_max_retries=3,
            start_request_retry_cooldown_seconds=1.0,
            request_event_timeout_seconds=2.0,
            logger=logger,
        )

        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=WarehouseOperation(
                    name="pickup_pallet",
                    payload=ExampleOperationPayload(
                        source_station="INBOUND_A",
                        target_station="BUFFER_01",
                        pallet_id="PALLET-1001",
                        run_seconds=8.0,
                    ),
                ),
                resource_id="robot-1",
                priority=10,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                operation=WarehouseOperation(
                    name="dropoff_pallet",
                    payload=ExampleOperationPayload(
                        source_station="BUFFER_01",
                        target_station="OUTBOUND_B",
                        pallet_id="PALLET-1001",
                        run_seconds=6.0,
                    ),
                ),
                resource_id="robot-1",
                priority=5,
            )
        )

    def _on_request_handler(self, event: DispatchEvent) -> bool | None:
        if event.event_type is EventType.OPERATION_START_REQUESTED:
            return self._allow_start_with_one_initial_denial(event)

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulated_runner.cancel(operation_id)
                return True

        if event.event_type is EventType.OPERATION_STOP_REQUESTED:
            current = self.operation_dispatcher.current_scheduled_operation
            if current is not None:
                self._simulated_runner.pause(current.operation.id)
                return False

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            current = self.operation_dispatcher.current_scheduled_operation
            if current is not None:
                self._simulated_runner.resume(current.operation.id)
            return True

        return None

    def _on_notification_handler(self, event: DispatchEvent) -> None:
        if event.event_type is EventType.OPERATION_STARTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulate_operation_run(operation_id)

        if event.event_type is EventType.OPERATION_START_DENIED:
            if self._logger is not None:
                self._logger.info(
                    "start denied for operation %s with metadata=%s",
                    event.operation_id,
                    event.data,
                )

        if event.event_type is EventType.OPERATION_CANCELLED:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulated_runner.cancel(operation_id)

        if event.event_type in {
            EventType.OPERATION_COMPLETED,
            EventType.OPERATION_FAILED,
            EventType.OPERATION_STOPPED,
        }:
            operation_id = event.operation_id
            if operation_id is not None:
                self._simulated_runner.cancel(operation_id)

    def _allow_start_with_one_initial_denial(self, event: DispatchEvent) -> bool:
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
        current = self.operation_dispatcher.current_scheduled_operation
        if current is not None and current.operation.id == operation_id:
            self.operation_dispatcher.complete_current()

    def _resolve_simulated_run_seconds(self, operation_id: UUID) -> float:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is None or current.operation.id != operation_id:
            return 5.0

        run_seconds = current.operation.payload.run_seconds
        if run_seconds > 0:
            return float(run_seconds)

        return 5.0

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
                            "simulation": "a dedicated simulated worker thread supports true pause/resume for /operation_dispatcher/current_operation/stop and /operation_dispatcher/current_operation/resume",
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
                        "stop_current_operation": "/operation_dispatcher/current_operation/stop",
                        "resume_current_operation": "/operation_dispatcher/current_operation/resume",
                        "get_dispatcher_state": "/operation_dispatcher/state",
                        "start": "/operation_dispatcher/start",
                        "stop": "/operation_dispatcher/stop",
                        "pause": "/operation_dispatcher/pause",
                        "resume": "/operation_dispatcher/resume",
                    }
                ),
                200,
            )

        operation_dispatcher_api.register_default_endpoints(app)

        return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    demo_dispatcher = DemoDispatcherService(logger=logger)
    app = demo_dispatcher.create_app()
    logger.info("starting Flask demo on http://localhost:8000")
    logger.info("open docs at http://localhost:8000/docs/")
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
