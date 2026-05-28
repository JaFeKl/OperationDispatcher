from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable
from typing import Any

from .state_store import DispatcherStateStore


class DispatcherMutationService:
    def __init__(self, state_store: DispatcherStateStore) -> None:
        self._state_store = state_store

    def execute(self, mutation: Callable[[], Any]) -> Any:
        runtime_loop = self._state_store.runtime_loop
        if runtime_loop is None or not self.is_running:
            return mutation()

        if not runtime_loop.is_running():
            return mutation()

        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is runtime_loop:
                return mutation()
        except RuntimeError:
            pass

        result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)
        done_event = threading.Event()

        def run_mutation() -> None:
            try:
                result_queue.put((True, mutation()))
            except Exception as error:
                result_queue.put((False, error))
            finally:
                done_event.set()

        runtime_loop.call_soon_threadsafe(run_mutation)
        done_event.wait()

        success, payload = result_queue.get_nowait()
        if success:
            return payload
        raise payload  # type: ignore[misc]

    @property
    def is_running(self) -> bool:
        runtime_loop = self._state_store.runtime_loop
        return runtime_loop is not None and runtime_loop.is_running()
