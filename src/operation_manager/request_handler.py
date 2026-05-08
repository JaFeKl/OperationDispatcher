from __future__ import annotations

import asyncio
import inspect
import queue
import threading
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID

from .models import Operation, OperationManagerEvent, OperationManagerEventType
from .retry_policy import RetryPolicy


class RequestHandler:
    def __init__(
        self,
        on_request_callback: Callable[[OperationManagerEvent], object] | None,
        request_retry_policy: RetryPolicy,
        request_event_timeout_seconds: float,
        append_event_history: Callable[[OperationManagerEvent], None],
        emit_event: Callable[
            [OperationManagerEventType, Operation | None, dict[str, Any] | None],
            object,
        ],
        log_event: Callable[[OperationManagerEvent], None],
        notify_wakeup: Callable[[], None],
        pause: Callable[[], None],
    ) -> None:
        self._on_request_callback = on_request_callback
        self._request_retry_policy = request_retry_policy
        self._request_event_timeout_seconds = request_event_timeout_seconds
        self._append_event_history = append_event_history
        self._emit_event = emit_event
        self._log_event = log_event
        self._notify_wakeup = notify_wakeup
        self._pause = pause

    async def request_operation_start(self, operation: Operation) -> bool:
        return await self.request_operation_with_retry(
            operation,
            OperationManagerEventType.OPERATION_START_REQUESTED,
        )

    async def request_operation_start_dispatch(self, operation: Operation) -> bool:
        return await self.request_operation_with_retry(
            operation,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        )

    def has_request_cooldown(self, operation: Operation) -> bool:
        now = datetime.now(timezone.utc)
        request_event_types = (
            OperationManagerEventType.OPERATION_START_REQUESTED,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        )
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(operation.id, request_event_type)
            if self._request_retry_policy.is_cooldown_active(retry_key, now):
                return True
        return False

    def clear_request_retry_state(self, operation_id: UUID) -> None:
        request_event_types = (
            OperationManagerEventType.OPERATION_START_REQUESTED,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        )
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(operation_id, request_event_type)
            self._request_retry_policy.clear(retry_key)

    def clear_all_request_retry_state(self) -> None:
        self._request_retry_policy.clear_all()

    def request_cooldown_wait_seconds(
        self,
        operation: Operation,
        now: datetime,
    ) -> float:
        cooldown_wait_seconds = 0.0
        request_event_types = (
            OperationManagerEventType.OPERATION_START_REQUESTED,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        )
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(operation.id, request_event_type)
            wait_seconds = self._request_retry_policy.cooldown_wait_seconds(
                retry_key,
                now,
            )
            cooldown_wait_seconds = max(cooldown_wait_seconds, wait_seconds)
        return cooldown_wait_seconds

    def request_operation_with_retry_sync(
        self,
        operation: Operation,
        event_type: OperationManagerEventType,
    ) -> bool:
        now = datetime.now(timezone.utc)
        retry_key = self._request_retry_key(operation.id, event_type)
        if self._request_retry_policy.is_cooldown_active(retry_key, now):
            return False

        is_allowed = self._request_operation_event_sync(operation, event_type)
        if is_allowed:
            self._request_retry_policy.clear(retry_key)
            return True

        metadata, max_retries_reached = self._request_retry_policy.on_denied(
            retry_key,
            now,
        )

        denied_event_type = self._denied_event_type(event_type)
        if denied_event_type is not None:
            self._emit_event(
                denied_event_type,
                operation,
                metadata,
            )

        if max_retries_reached:
            self._pause()
        return False

    async def request_operation_with_retry(
        self,
        operation: Operation,
        event_type: OperationManagerEventType,
    ) -> bool:
        now = datetime.now(timezone.utc)
        retry_key = self._request_retry_key(operation.id, event_type)
        if self._request_retry_policy.is_cooldown_active(retry_key, now):
            return False

        is_allowed = await self._request_operation_event(operation, event_type)
        if is_allowed:
            self._request_retry_policy.clear(retry_key)
            return True

        metadata, max_retries_reached = self._request_retry_policy.on_denied(
            retry_key,
            now,
        )

        denied_event_type = self._denied_event_type(event_type)
        if denied_event_type is not None:
            self._emit_event(
                denied_event_type,
                operation,
                metadata,
            )

        if max_retries_reached:
            self._pause()
        return False

    def _request_retry_key(
        self,
        operation_id: UUID,
        event_type: OperationManagerEventType,
    ) -> tuple[OperationManagerEventType, UUID]:
        return (event_type, operation_id)

    @staticmethod
    def _denied_event_type(
        requested_event_type: OperationManagerEventType,
    ) -> OperationManagerEventType | None:
        denied_event_type_by_requested_event_type = {
            OperationManagerEventType.OPERATION_START_REQUESTED: OperationManagerEventType.OPERATION_START_DENIED,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED: OperationManagerEventType.OPERATION_START_DISPATCH_DENIED,
            OperationManagerEventType.OPERATION_CANCEL_REQUESTED: OperationManagerEventType.OPERATION_CANCEL_DENIED,
            OperationManagerEventType.OPERATION_STOP_REQUESTED: OperationManagerEventType.OPERATION_STOP_DENIED,
            OperationManagerEventType.OPERATION_RESUME_REQUESTED: OperationManagerEventType.OPERATION_RESUME_DENIED,
        }
        return denied_event_type_by_requested_event_type.get(requested_event_type)

    async def _request_operation_event(
        self,
        operation: Operation,
        event_type: OperationManagerEventType,
    ) -> bool:
        event = OperationManagerEvent(
            event_type=event_type,
            agent_id=operation.agent_id,
            operation_id=operation.id,
            operation_name=operation.name,
            data={},
        )
        self._append_event_history(event)

        is_allowed = self._on_request_callback is None
        if self._on_request_callback is not None:
            try:
                callback_result = await self._invoke_request_callback_async(event)
                is_allowed = callback_result is True
            except Exception:
                is_allowed = False

        self._log_event(event)
        self._notify_wakeup()
        return is_allowed

    def _request_operation_event_sync(
        self,
        operation: Operation,
        event_type: OperationManagerEventType,
    ) -> bool:
        event = OperationManagerEvent(
            event_type=event_type,
            agent_id=operation.agent_id,
            operation_id=operation.id,
            operation_name=operation.name,
            data={},
        )
        self._append_event_history(event)

        is_allowed = self._on_request_callback is None
        if self._on_request_callback is not None:
            try:
                callback_result = self._invoke_request_callback_sync(event)
                is_allowed = callback_result is True
            except Exception:
                is_allowed = False

        self._log_event(event)
        self._notify_wakeup()
        return is_allowed

    async def _invoke_request_callback_async(
        self, event: OperationManagerEvent
    ) -> object:
        if self._on_request_callback is None:
            raise RuntimeError("on_request_callback is not configured")

        callback_result = await asyncio.wait_for(
            asyncio.to_thread(self._on_request_callback, event),
            timeout=self._request_event_timeout_seconds,
        )
        if inspect.isawaitable(callback_result):
            callback_result = await asyncio.wait_for(
                _as_awaitable(callback_result),
                timeout=self._request_event_timeout_seconds,
            )
        return callback_result

    def _invoke_request_callback_sync(self, event: OperationManagerEvent) -> object:
        if self._on_request_callback is None:
            raise RuntimeError("on_request_callback is not configured")

        result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

        def run_callback() -> None:
            try:
                callback_result = self._on_request_callback(event)
                if inspect.isawaitable(callback_result):
                    callback_result = asyncio.run(
                        asyncio.wait_for(
                            _as_awaitable(callback_result),
                            timeout=self._request_event_timeout_seconds,
                        )
                    )
                result_queue.put((True, callback_result))
            except Exception as error:
                result_queue.put((False, error))

        callback_thread = threading.Thread(target=run_callback, daemon=True)
        callback_thread.start()
        callback_thread.join(timeout=self._request_event_timeout_seconds)
        if callback_thread.is_alive():
            raise TimeoutError("request callback timed out")

        success, payload = result_queue.get_nowait()
        if success:
            return payload

        raise payload  # type: ignore[misc]


def _as_awaitable(result: object) -> Awaitable[object]:
    return cast(Awaitable[object], result)
