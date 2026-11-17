from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
import inspect
import queue
import threading
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID

from .models import (
    DispatchEvent,
    EventType,
    Operation,
)
from .retry_policy import RetryPolicy


@dataclass(slots=True, frozen=True)
class RequestDecision:
    accepted: bool
    reasoning: str | None = None
    data: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if type(self.accepted) is not bool:
            raise TypeError("accepted must be a boolean")
        if self.reasoning is not None and not isinstance(self.reasoning, str):
            raise TypeError("reasoning must be a string or None")
        if self.data is not None and not isinstance(self.data, dict):
            raise TypeError("data must be an object or None")


RequestCallback = Callable[
    [DispatchEvent],
    bool
    | RequestDecision
    | dict[str, Any]
    | None
    | Awaitable[bool | RequestDecision | dict[str, Any] | None],
]


class RequestHandler:
    def __init__(
        self,
        on_request_callback: RequestCallback | None,
        request_retry_policy: RetryPolicy,
        request_event_timeout_seconds: float,
        emit_event: Callable[..., object],
        pause: Callable[[], None],
        execute_mutation: Callable[[Callable[[], Any]], Any],
    ) -> None:
        self._on_request_callback = on_request_callback
        self._request_retry_policy = request_retry_policy
        self._request_event_timeout_seconds = request_event_timeout_seconds
        self._emit_event = emit_event
        self._pause = pause
        self._execute_mutation = execute_mutation

    async def request_operation_start(self, operation: Operation) -> bool:
        return await self.request_operation_with_retry(
            operation,
            EventType.OPERATION_START_REQUESTED,
            meta_data=None,
        )

    def has_request_cooldown(self, operation: Operation) -> bool:
        now = datetime.now(timezone.utc)
        request_event_types = (EventType.OPERATION_START_REQUESTED,)
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(
                operation.id,
                request_event_type,
            )
            if self._request_retry_policy.is_cooldown_active(retry_key, now):
                return True
        return False

    def clear_request_retry_state(self, operation_id: UUID) -> None:
        request_event_types = (
            EventType.OPERATION_START_REQUESTED,
            EventType.OPERATION_CANCEL_REQUESTED,
            EventType.OPERATION_PAUSE_REQUESTED,
            EventType.OPERATION_RESUME_REQUESTED,
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
        request_event_types = (EventType.OPERATION_START_REQUESTED,)
        for request_event_type in request_event_types:
            retry_key = self._request_retry_key(
                operation.id,
                request_event_type,
            )
            wait_seconds = self._request_retry_policy.cooldown_wait_seconds(
                retry_key,
                now,
            )
            cooldown_wait_seconds = max(cooldown_wait_seconds, wait_seconds)
        return cooldown_wait_seconds

    def can_request(self, operation: Operation, event_type: EventType) -> bool:
        return not self._request_retry_policy.is_cooldown_active(
            self._request_retry_key(operation.id, event_type),
            datetime.now(timezone.utc),
        )

    def request_operation_with_retry_sync(
        self,
        operation: Operation,
        event_type: EventType,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        if not self._execute_mutation(lambda: self.can_request(operation, event_type)):
            return False
        decision = self.evaluate_operation_request_sync(
            operation, event_type, meta_data
        )
        return self._execute_mutation(
            lambda: self.complete_request(operation, event_type, decision, meta_data)
        )

    async def request_operation_with_retry(
        self,
        operation: Operation,
        event_type: EventType,
        meta_data: dict[str, Any] | None = None,
    ) -> bool:
        if not self._execute_mutation(lambda: self.can_request(operation, event_type)):
            return False
        decision = await self._request_operation_event(operation, event_type, meta_data)
        return self._execute_mutation(
            lambda: self.complete_request(operation, event_type, decision, meta_data)
        )

    def complete_request(
        self,
        operation: Operation,
        event_type: EventType,
        decision: RequestDecision,
        meta_data: dict[str, Any] | None = None,
        *,
        use_retry: bool = True,
    ) -> bool:
        """Record a decision and retry effects while the caller owns mutation access."""
        self.record_request_decision(operation, event_type, decision, meta_data)
        retry_key = self._request_retry_key(operation.id, event_type)
        if decision.accepted:
            self._request_retry_policy.clear(retry_key)
            return True
        metadata, exhausted = ({}, False)
        if use_retry:
            metadata, exhausted = self._request_retry_policy.on_denied(
                retry_key, datetime.now(timezone.utc)
            )
        if decision.reasoning is not None:
            metadata["reasoning"] = decision.reasoning
        if decision.data:
            metadata["decision_data"] = deepcopy(decision.data)
        if meta_data:
            metadata["request_meta_data"] = deepcopy(meta_data)
        denied = self._denied_event_type(event_type)
        if denied is not None:
            self._emit_event(denied, operation, metadata)
        if exhausted:
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
            EventType.OPERATION_COMPLETE_REQUESTED: EventType.OPERATION_COMPLETE_DENIED,
        }
        return denied_event_type_by_requested_event_type.get(requested_event_type)

    async def _request_operation_event(
        self,
        operation: Operation,
        event_type: EventType,
        meta_data: dict[str, Any] | None = None,
    ) -> RequestDecision:
        resolved_meta_data = {} if meta_data is None else deepcopy(meta_data)
        callback_event = DispatchEvent(
            resource_id=operation.resource_id,
            operation_id=operation.id,
            event_type=event_type,
            meta_data=resolved_meta_data,
        )

        decision = RequestDecision(accepted=True)
        if self._on_request_callback is not None:
            try:
                callback_result = await self._invoke_request_callback_async(
                    callback_event
                )
                decision = self._resolve_request_decision(callback_result)
            except Exception as error:
                decision = RequestDecision(
                    accepted=False,
                    reasoning="request_callback_exception",
                    data={"error": str(error)},
                )

        return decision

    def evaluate_operation_request_sync(
        self,
        operation: Operation,
        event_type: EventType,
        meta_data: dict[str, Any] | None = None,
        *,
        require_callback: bool = False,
    ) -> RequestDecision:
        """Obtain a decision without mutating dispatcher state or retry counters."""
        resolved_meta_data = {} if meta_data is None else deepcopy(meta_data)
        callback_event = DispatchEvent(
            resource_id=operation.resource_id,
            operation_id=operation.id,
            event_type=event_type,
            meta_data=resolved_meta_data,
        )

        decision = RequestDecision(
            accepted=not require_callback,
            reasoning="request_callback_not_configured" if require_callback else None,
        )
        if self._on_request_callback is not None:
            try:
                callback_result = self._invoke_request_callback_sync(callback_event)
                decision = self._resolve_request_decision(callback_result)
            except Exception as error:
                decision = RequestDecision(
                    accepted=False,
                    reasoning="request_callback_exception",
                    data={"error": str(error)},
                )

        return decision

    def record_request_decision(
        self,
        operation: Operation,
        event_type: EventType,
        decision: RequestDecision,
        meta_data: dict[str, Any] | None = None,
    ) -> None:
        request_event_meta_data = {
            **(meta_data or {}),
            **self._decision_event_meta_data(decision),
        }
        self._emit_event(
            event_type,
            operation,
            request_event_meta_data,
            notify=False,
        )

    @staticmethod
    def _resolve_request_decision(callback_result: object) -> RequestDecision:
        try:
            if isinstance(callback_result, RequestDecision):
                return deepcopy(callback_result)
            if type(callback_result) is bool:
                return RequestDecision(accepted=callback_result)
            if isinstance(callback_result, dict):
                return RequestDecision(
                    accepted=callback_result.get("accepted"),
                    reasoning=callback_result.get("reasoning"),
                    data=deepcopy(callback_result.get("data")),
                )
        except (TypeError, ValueError):
            pass
        return RequestDecision(
            accepted=False,
            reasoning="invalid_callback_result",
            data={"result_type": type(callback_result).__name__},
        )

    @staticmethod
    def _decision_event_meta_data(decision: RequestDecision) -> dict[str, Any]:
        return {
            "request_decision": {
                "accepted": decision.accepted,
                "reasoning": decision.reasoning,
                "data": decision.data or {},
            }
        }

    async def _invoke_request_callback_async(self, event: DispatchEvent) -> object:
        if self._on_request_callback is None:
            raise RuntimeError("on_request_callback is not configured")

        async def invoke() -> object:
            if inspect.iscoroutinefunction(self._on_request_callback):
                return await self._on_request_callback(event)
            # The bounded worker also prevents a timed-out synchronous callback
            # from holding asyncio's executor shutdown open indefinitely.
            return await asyncio.to_thread(self._invoke_request_callback_sync, event)

        return await asyncio.wait_for(
            invoke(), timeout=self._request_event_timeout_seconds
        )

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
