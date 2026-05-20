from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from uuid import UUID


class SimulatedOperationRunner:
    def __init__(
        self,
        on_complete: Callable[[UUID], None],
        logger: logging.Logger | None = None,
        tick_seconds: float = 0.05,
        progress_log_interval_seconds: float = 1.0,
    ) -> None:
        self._on_complete = on_complete
        self._logger = logger
        self._tick_seconds = tick_seconds
        self._progress_log_interval_seconds = progress_log_interval_seconds

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
        next_progress_log_time = last_tick

        while True:
            if self._stop_event.is_set():
                return

            if not self._pause_event.wait(timeout=self._tick_seconds):
                last_tick = time.monotonic()
                continue

            now = time.monotonic()
            elapsed_seconds = max(0.0, now - last_tick)
            last_tick = now
            progress_snapshot: tuple[UUID, float] | None = None

            with self._lock:
                if self._operation_id is None:
                    return

                self._remaining_seconds -= elapsed_seconds
                if self._remaining_seconds > 0:
                    if (
                        self._logger is not None
                        and self._progress_log_interval_seconds > 0
                        and now >= next_progress_log_time
                    ):
                        progress_snapshot = (
                            self._operation_id,
                            self._remaining_seconds,
                        )
                        next_progress_log_time = (
                            now + self._progress_log_interval_seconds
                        )

                    if progress_snapshot is not None:
                        operation_id, remaining_seconds = progress_snapshot
                        self._logger.info(
                            "simulated operation %s is running (remaining_seconds=%.2f)",
                            operation_id,
                            remaining_seconds,
                        )
                    continue

                operation_id = self._operation_id
                self._operation_id = None
                self._remaining_seconds = 0.0
                self._worker_thread = None

            if progress_snapshot is not None and self._logger is not None:
                progress_operation_id, remaining_seconds = progress_snapshot
                self._logger.info(
                    "simulated operation %s is running (remaining_seconds=%.2f)",
                    progress_operation_id,
                    remaining_seconds,
                )

            try:
                self._on_complete(operation_id)
            except Exception:
                if self._logger is not None:
                    self._logger.exception(
                        "simulated operation completion callback failed"
                    )
            return
