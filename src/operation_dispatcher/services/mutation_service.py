from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TypeVar

from .state_store import DispatcherStateStore

_T = TypeVar("_T")


class DispatcherMutationService:
    """Serialize short state changes without depending on a running event loop.

    Application approval callbacks must execute outside this lock. This lets
    callbacks reenter the dispatcher and lets shutdown race safely with callers.
    """

    def __init__(self, state_store: DispatcherStateStore) -> None:
        self._state_store = state_store
        self._lock = threading.RLock()

    def execute(self, mutation: Callable[[], _T]) -> _T:
        with self._lock:
            return mutation()

    @property
    def is_running(self) -> bool:
        runtime_loop = self._state_store.runtime_loop
        return runtime_loop is not None and runtime_loop.is_running()
