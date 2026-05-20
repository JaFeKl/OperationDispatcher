from __future__ import annotations

import asyncio
import inspect
import queue
import threading
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID

from .models import (
    DispatchEvent,
    EventData,
    EventType,
    RequestDecision,
    ScheduledOperation,
)
from .retry_policy import RetryPolicy


class RequestHandler:
    def __init__(
        self,
        on_request_callback: Callable[[DispatchEvent], object] | None,
        request_retry_policy: RetryPolicy,
        request_event_timeout_seconds: float,
        append_event_history: Callable[[DispatchEvent], None],
        append_operation_event: Callable[[UUID, DispatchEvent], None],
        emit_event: Callable[
            [
                EventType,
                ScheduledOperation | None,
                dict[str, Any] | None,
            ],
            object,
        ],
        log_event: Callable[[DispatchEvent], None],
        notify_wakeup: Callable[[], None],
        pause: Callable[[], None],
    ) -> None:
        self._on_request_callback = on_request_callback
        self._request_retry_policy = request_retry_policy
        self._request_event_timeout_seconds = request_event_timeout_seconds
        self._append_event_history = append_event_history
        self._append_operation_event = append_operation_event
        self._emit_event = emit_event
        self._log_event = log_event
        self._notify_wakeup = notify_wakeup
        self._pause = pause

    async def request_operation_start(self, operation: ScheduledOperation) -> bool:
        return await self.request_operation_with_retry(
            operation,
            EventType.OPERATION_START_REQUESTED,
        )

    def has_request_cooldown(self, operation: ScheduledOperation) -> bool:
        now = datetime.now(timezone.utc)
        request_event_types = (EventType.OPERATION_START_REQUESTED,)
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(
                operation.operation.id,
                request_event_type,
            )
            if self._request_retry_policy.is_cooldown_active(retry_key, now):
                return True
        return False

    def clear_request_retry_state(self, operation_id: UUID) -> None:
        request_event_types = (EventType.OPERATION_START_REQUESTED,)
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(operation_id, request_event_type)
            self._request_retry_policy.clear(retry_key)

    def clear_all_request_retry_state(self) -> None:
        self._request_retry_policy.clear_all()

    def request_cooldown_wait_seconds(
        self,
        operation: ScheduledOperation,
        now: datetime,
    ) -> float:
        cooldown_wait_seconds = 0.0
        request_event_types = (EventType.OPERATION_START_REQUESTED,)
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(
                operation.operation.id,
                request_event_type,
            )
            wait_seconds = self._request_retry_policy.cooldown_wait_seconds(
                retry_key,
                now,
            )
            cooldown_wait_seconds = max(cooldown_wait_seconds, wait_seconds)
        return cooldown_wait_seconds

    def request_operation_with_retry_sync(
        self,
        operation: ScheduledOperation,
        event_type: EventType,
    ) -> bool:
        now = datetime.now(timezone.utc)
        retry_key = self._request_retry_key(operation.operation.id, event_type)
        if self._request_retry_policy.is_cooldown_active(retry_key, now):
            return False

        decision = self._request_operation_event_sync(operation, event_type)
        if decision.is_allowed:
            self._request_retry_policy.clear(retry_key)
            return True

        metadata, max_retries_reached = self._request_retry_policy.on_denied(
            retry_key,
            now,
        )

        denied_event_type = self._denied_event_type(event_type)
        if denied_event_type is not None:
            denied_metadata = dict(metadata)
            if decision.reason is not None:
                denied_metadata["reason"] = decision.reason
            if decision.metadata:
                denied_metadata["decision_metadata"] = decision.metadata
            self._emit_event(
                denied_event_type,
                operation,
                denied_metadata,
            )

        if max_retries_reached:
            self._pause()
        return False

    async def request_operation_with_retry(
        self,
        operation: ScheduledOperation,
        event_type: EventType,
    ) -> bool:
        now = datetime.now(timezone.utc)
        retry_key = self._request_retry_key(operation.operation.id, event_type)
        if self._request_retry_policy.is_cooldown_active(retry_key, now):
            return False

        decision = await self._request_operation_event(operation, event_type)
        if decision.is_allowed:
            self._request_retry_policy.clear(retry_key)
            return True

        metadata, max_retries_reached = self._request_retry_policy.on_denied(
            retry_key,
            now,
        )

        denied_event_type = self._denied_event_type(event_type)
        if denied_event_type is not None:
            denied_metadata = dict(metadata)
            if decision.reason is not None:
                denied_metadata["reason"] = decision.reason
            if decision.metadata:
                denied_metadata["decision_metadata"] = decision.metadata
            self._emit_event(
                denied_event_type,
                operation,
                denied_metadata,
            )

        if max_retries_reached:
            self._pause()
        return False

    def _request_retry_key(
        self,
        operation_id: UUID,
        event_type: EventType,
    ) -> tuple[EventType, UUID]:
        return (event_type, operation_id)

    @staticmethod
    def _denied_event_type(
        requested_event_type: EventType,
    ) -> EventType | None:
        denied_event_type_by_requested_event_type = {
            EventType.OPERATION_START_REQUESTED: EventType.OPERATION_START_DENIED,
            EventType.OPERATION_CANCEL_REQUESTED: EventType.OPERATION_CANCEL_DENIED,
            EventType.OPERATION_PAUSE_REQUESTED: EventType.OPERATION_PAUSE_DENIED,
            EventType.OPERATION_RESUME_REQUESTED: EventType.OPERATION_RESUME_DENIED,
        }
        return denied_event_type_by_requested_event_type.get(requested_event_type)

    async def _request_operation_event(
        self,
        operation: ScheduledOperation,
        event_type: EventType,
    ) -> RequestDecision:
        event = DispatchEvent(
            event_type=event_type,
            resource_id=operation.resource_id,
            operation_id=operation.operation.id,
            data=EventData(),
        )
        self._append_event_history(event)

        decision = RequestDecision(is_allowed=True)
        if self._on_request_callback is not None:
            try:
                callback_result = await self._invoke_request_callback_async(event)
                decision = self._resolve_request_decision(callback_result)
            except Exception as error:
                decision = RequestDecision(
                    is_allowed=False,
                    reason="request_callback_exception",
                    metadata={"error": str(error)},
                )

        event.data = self._decision_event_data(decision)
        self._record_request_event(
            operation_id=operation.operation.id,
            event=event,
            append_operation_event=self._append_operation_event,
        )

        self._log_event(event)
        self._notify_wakeup()
        return decision

    def _request_operation_event_sync(
        self,
        operation: ScheduledOperation,
        event_type: EventType,
    ) -> RequestDecision:
        event = DispatchEvent(
            event_type=event_type,
            resource_id=operation.resource_id,
            operation_id=operation.operation.id,
            data=EventData(),
        )
        self._append_event_history(event)

        decision = RequestDecision(is_allowed=True)
        if self._on_request_callback is not None:
            try:
                callback_result = self._invoke_request_callback_sync(event)
                decision = self._resolve_request_decision(callback_result)
            except Exception as error:
                decision = RequestDecision(
                    is_allowed=False,
                    reason="request_callback_exception",
                    metadata={"error": str(error)},
                )

        event.data = self._decision_event_data(decision)
        self._record_request_event(
            operation_id=operation.operation.id,
            event=event,
            append_operation_event=self._append_operation_event,
        )

        self._log_event(event)
        self._notify_wakeup()
        return decision

    @staticmethod
    def _resolve_request_decision(callback_result: object) -> RequestDecision:
        if isinstance(callback_result, RequestDecision):
            return callback_result
        if callback_result is True:
            return RequestDecision(is_allowed=True)
        if callback_result is False:
            return RequestDecision(is_allowed=False)
        return RequestDecision(
            is_allowed=False,
            reason="invalid_callback_result",
            metadata={"result_type": type(callback_result).__name__},
        )

    @staticmethod
    def _decision_event_data(decision: RequestDecision) -> EventData:
        return EventData(request_decision=decision)

    @staticmethod
    def _record_request_event(
        operation_id: UUID,
        event: DispatchEvent,
        append_operation_event: Callable[[UUID, DispatchEvent], None],
    ) -> None:
        append_operation_event(operation_id, event)

    async def _invoke_request_callback_async(self, event: DispatchEvent) -> object:
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

    def _invoke_request_callback_sync(self, event: DispatchEvent) -> object:
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
