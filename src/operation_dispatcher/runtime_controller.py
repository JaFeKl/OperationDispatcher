from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from .operation_dispatcher import OperationDispatcher


class OperationDispatcherRuntimeController:
    """Runtime lifecycle controller for an OperationDispatcher."""

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        startup_timeout_seconds: float = 1.0,
        stop_join_timeout_seconds: float = 2.0,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._startup_timeout_seconds = startup_timeout_seconds
        self._stop_join_timeout_seconds = stop_join_timeout_seconds

        self._runtime_thread: threading.Thread | None = None
        self._runtime_last_error: str | None = None
        self._runtime_lock = threading.Lock()

    @property
    def runtime_thread(self) -> threading.Thread | None:
        return self._runtime_thread

    @property
    def runtime_last_error(self) -> str | None:
        return self._runtime_last_error

    def get_state_payload(self) -> dict[str, Any]:
        state = self._operation_dispatcher.get_state().model_dump(mode="json")
        state["runtime_thread_alive"] = (
            self._runtime_thread.is_alive()
            if self._runtime_thread is not None
            else False
        )
        state["runtime_last_error"] = self._runtime_last_error
        return state

    def start(self) -> tuple[dict[str, Any], int]:
        with self._runtime_lock:
            if self._operation_dispatcher.is_running:
                return {
                    "message": "operation dispatcher is already running",
                    "state": self.get_state_payload(),
                }, 409

            if self._runtime_thread is not None and self._runtime_thread.is_alive():
                return {
                    "message": "operation dispatcher runtime thread already active",
                    "state": self.get_state_payload(),
                }, 409

            self._runtime_last_error = None

            def run_operation_dispatcher() -> None:
                try:
                    asyncio.run(self._operation_dispatcher.run())
                except Exception as error:
                    self._runtime_last_error = str(error)

            self._runtime_thread = threading.Thread(
                target=run_operation_dispatcher,
                name="OperationDispatcherRuntimeThread",
                daemon=True,
            )
            self._runtime_thread.start()

        deadline = time.time() + self._startup_timeout_seconds
        while not self._operation_dispatcher.is_running and time.time() < deadline:
            time.sleep(0.01)

        state = self.get_state_payload()
        if self._operation_dispatcher.is_running:
            return {
                "message": "operation dispatcher started",
                "state": state,
            }, 202

        if self._runtime_last_error:
            return {
                "message": "operation dispatcher failed to start",
                "state": state,
                "error": self._runtime_last_error,
            }, 500

        return {
            "message": "operation dispatcher start requested",
            "state": state,
        }, 202

    def stop(self) -> tuple[dict[str, Any], int]:
        with self._runtime_lock:
            runtime_active = (
                self._runtime_thread is not None and self._runtime_thread.is_alive()
            )
            if not self._operation_dispatcher.is_running and not runtime_active:
                return {
                    "message": "operation dispatcher is not running",
                    "state": self.get_state_payload(),
                }, 409

            self._operation_dispatcher.request_stop()
            runtime_thread = self._runtime_thread

        if runtime_thread is not None and runtime_thread.is_alive():
            runtime_thread.join(timeout=self._stop_join_timeout_seconds)

        return {
            "message": "operation dispatcher stopped",
            "state": self.get_state_payload(),
        }, 202

    def pause(self) -> tuple[dict[str, Any], int]:
        if self._operation_dispatcher.is_paused:
            return {
                "message": "operation dispatcher is already paused",
                "state": self.get_state_payload(),
            }, 409

        self._operation_dispatcher.pause_dispatcher_runtime()

        return {
            "message": "operation dispatcher paused",
            "state": self.get_state_payload(),
        }, 200

    def resume(self) -> tuple[dict[str, Any], int]:
        if not self._operation_dispatcher.is_paused:
            return {
                "message": "operation dispatcher is not paused",
                "state": self.get_state_payload(),
            }, 409

        self._operation_dispatcher.resume_dispatcher_runtime()

        return {
            "message": "operation dispatcher resumed",
            "state": self.get_state_payload(),
        }, 200
