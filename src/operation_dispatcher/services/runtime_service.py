from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from operation_dispatcher.models import (
    EventType,
    ExecutionState,
    Operation,
    OperationDispatcherState,
)

from .event_service import DispatcherEventService
from .mutation_service import DispatcherMutationService
from .state_store import DispatcherStateStore


class DispatcherRuntimeService:
    def __init__(
        self,
        state_store: DispatcherStateStore,
        mutation_service: DispatcherMutationService,
        event_service: DispatcherEventService,
    ) -> None:
        self._state_store = state_store
        self._mutation_service = mutation_service
        self._event_service = event_service

    @property
    def is_paused(self) -> bool:
        return self._state_store.is_paused

    @property
    def is_running(self) -> bool:
        return self._mutation_service.is_running

    @property
    def is_stopping(self) -> bool:
        return self._state_store.stop_requested and self.is_running

    @property
    def current_operation(self) -> Operation | None:
        return self._state_store.dispatch_queue.pulled_operation

    def get_state(self) -> OperationDispatcherState:
        now = datetime.now(timezone.utc)
        running_since = self._state_store.running_since
        uptime_seconds: float | None = None
        if self.is_running and running_since is not None:
            uptime_seconds = (now - running_since).total_seconds()

        return OperationDispatcherState(
            is_running=self.is_running,
            is_paused=self._state_store.is_paused,
            queue_size=len(self._state_store.dispatch_queue),
            current_operation=self.current_operation,
            running_since=running_since,
            uptime_seconds=uptime_seconds,
        )

    def pause_dispatcher_runtime(self) -> None:
        self._mutation_service.execute(self._pause_internal)

    def _pause_internal(self) -> None:
        self._state_store.is_paused = True
        current = self.current_operation
        if current is not None:
            current.state = ExecutionState.PAUSED
        self._event_service.emit_event(EventType.OPERATION_DISPATCHER_PAUSED)

    def resume_dispatcher_runtime(self) -> None:
        self._mutation_service.execute(self._resume_internal)

    def _resume_internal(self) -> None:
        request_handler = self._state_store.request_handler
        if request_handler is not None:
            request_handler.clear_all_request_retry_state()
        self._state_store.is_paused = False
        self._event_service.emit_event(EventType.OPERATION_DISPATCHER_RESUMED)

    def request_stop(self) -> None:
        self._mutation_service.execute(self._request_stop_internal)

    def _request_stop_internal(self) -> None:
        self._state_store.stop_requested = True
        self.notify_wakeup()

    async def step_dispatch(self) -> Operation | None:
        if self._state_store.is_paused or self.current_operation is not None:
            return None

        next_operation = self._state_store.dispatch_queue.peek()
        if next_operation is None:
            return None

        if next_operation.release_date is not None:
            now = datetime.now(timezone.utc)
            if next_operation.release_date > now:
                return None

        request_handler = self._state_store.request_handler
        if request_handler is None:
            return None

        if request_handler.has_request_cooldown(next_operation):
            return None

        if not await request_handler.request_operation_start(next_operation):
            return None

        return self._start_next()

    def _start_next(self) -> Operation | None:
        if self._state_store.is_paused:
            return None

        if self.current_operation is not None:
            raise RuntimeError("an operation is already running")

        operation = self._state_store.dispatch_queue.next()
        if operation is None:
            return None

        old_operation = operation.model_copy(deep=True)
        operation.state = ExecutionState.RUNNING
        if operation.start_time is None:
            operation.start_time = datetime.now(timezone.utc)

        self._event_service.emit_event(
            EventType.OPERATION_STARTED,
            operation=operation,
            old_operation=old_operation,
        )
        return operation

    async def run(self, start_paused: bool) -> None:
        if self.is_running:
            raise RuntimeError("operation dispatcher is already running")

        self._state_store.running_since = datetime.now(timezone.utc)
        self._state_store.stop_requested = False
        self._state_store.runtime_loop = asyncio.get_running_loop()
        self._state_store.wakeup_event = asyncio.Event()
        self._event_service.emit_event(EventType.OPERATION_DISPATCHER_STARTED)
        if start_paused:
            self._event_service.emit_event(EventType.OPERATION_DISPATCHER_PAUSED)
        try:
            while not self._state_store.stop_requested:
                try:
                    await self.step_dispatch()
                except Exception as error:
                    logger = self._state_store.logger
                    if logger is not None:
                        logger.exception(
                            "operation dispatcher loop iteration failed",
                            exc_info=error,
                        )

                if self._state_store.stop_requested:
                    break

                await self._wait_for_next_signal()
        finally:
            self._state_store.running_since = None
            self._event_service.emit_event(EventType.OPERATION_DISPATCHER_STOPPED)
            self._state_store.runtime_loop = None
            self._state_store.wakeup_event = None

    async def _wait_for_next_signal(self) -> None:
        wait_seconds = self._next_wait_seconds()
        if wait_seconds == 0:
            await asyncio.sleep(0)
            return

        wakeup_event = self._state_store.wakeup_event
        if wakeup_event is None:
            await asyncio.sleep(self._state_store.poll_interval_seconds)
            return

        if wait_seconds is None:
            await wakeup_event.wait()
            wakeup_event.clear()
            return

        try:
            await asyncio.wait_for(
                wakeup_event.wait(),
                timeout=wait_seconds,
            )
        except TimeoutError:
            pass
        finally:
            wakeup_event.clear()

    def _next_wait_seconds(self) -> float | None:
        if self._state_store.is_paused:
            return None

        next_operation = self._state_store.dispatch_queue.peek()
        if next_operation is None:
            return None

        now = datetime.now(timezone.utc)

        release_wait_seconds = 0.0
        if next_operation.release_date is not None:
            release_wait_seconds = (next_operation.release_date - now).total_seconds()
            if release_wait_seconds < 0:
                release_wait_seconds = 0.0

        request_handler = self._state_store.request_handler
        cooldown_wait_seconds = 0.0
        if request_handler is not None:
            cooldown_wait_seconds = request_handler.request_cooldown_wait_seconds(
                next_operation,
                now,
            )

        return max(release_wait_seconds, cooldown_wait_seconds)

    def notify_wakeup(self) -> None:
        if not self.is_running:
            return

        runtime_loop = self._state_store.runtime_loop
        wakeup_event = self._state_store.wakeup_event
        if runtime_loop is None or wakeup_event is None:
            return

        runtime_loop.call_soon_threadsafe(wakeup_event.set)
