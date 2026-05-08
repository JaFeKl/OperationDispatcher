from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable
from typing import cast
import logging

from .models import OperationManagerEvent


class NotificationHandler:
    def __init__(
        self,
        on_notification_callback: Callable[[OperationManagerEvent], object] | None,
        runtime_loop_getter: Callable[[], asyncio.AbstractEventLoop | None],
        logger: logging.Logger | None = None,
    ) -> None:
        self._on_notification_callback = on_notification_callback
        self._runtime_loop_getter = runtime_loop_getter
        self._logger = logger

    def notify(self, event: OperationManagerEvent) -> None:
        if self._on_notification_callback is None:
            return

        try:
            callback_result = self._on_notification_callback(event)
            if inspect.isawaitable(callback_result):
                self._schedule_awaitable(_as_awaitable(callback_result))
        except Exception:
            pass

    def _schedule_awaitable(self, awaitable: Awaitable[object]) -> None:
        try:
            running_loop = asyncio.get_running_loop()
            running_loop.create_task(awaitable)
            return
        except RuntimeError:
            pass

        runtime_loop = self._runtime_loop_getter()
        if runtime_loop is None:
            self._run_awaitable_in_background(awaitable)
            return

        if not runtime_loop.is_running():
            self._run_awaitable_in_background(awaitable)
            return

        runtime_loop.call_soon_threadsafe(
            runtime_loop.create_task,
            awaitable,
        )

    def _run_awaitable_in_background(self, awaitable: Awaitable[object]) -> None:
        def run_awaitable() -> None:
            try:
                asyncio.run(_as_awaitable(awaitable))
            except Exception as error:
                if self._logger is not None:
                    self._logger.exception(
                        "background awaitable execution failed",
                        exc_info=error,
                    )

        threading.Thread(target=run_awaitable, daemon=True).start()


def _as_awaitable(result: object) -> Awaitable[object]:
    return cast(Awaitable[object], result)
