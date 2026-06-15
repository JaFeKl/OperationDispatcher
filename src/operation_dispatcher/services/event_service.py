from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from operation_dispatcher.models import DispatchEvent, EventType, Operation
from operation_dispatcher.utils import get_changes

from .state_store import DispatcherStateStore


class DispatcherEventService:
    def __init__(
        self,
        state_store: DispatcherStateStore,
        notify_wakeup: Callable[[], None],
    ) -> None:
        self._state_store = state_store
        self._notify_wakeup = notify_wakeup

    def emit_event(
        self,
        event_type: EventType,
        operation: Operation | None = None,
        meta_data: dict[str, Any] | None = None,
        old_operation: Operation | None = None,
        notify: bool = True,
    ) -> DispatchEvent:
        resolved_meta_data = {} if meta_data is None else dict(meta_data)
        resolved_changes = (
            get_changes(old_operation, operation)
            if old_operation is not None and operation is not None
            else []
        )

        resource_id = (
            operation.resource_id
            if operation is not None
            else self._state_store.dispatch_queue.resource_id
        )
        operation_id = operation.id if operation is not None else None
        event = DispatchEvent(
            resource_id=resource_id,
            operation_id=operation_id,
            event_type=event_type,
            changes=resolved_changes,
            meta_data=resolved_meta_data,
        )
        self.append_event_history(event)
        self.log_event(event)
        notification_handler = self._state_store.notification_handler
        if notify and notification_handler is not None:
            notification_handler.notify(event)
        self._notify_wakeup()
        return event

    def append_event_history(self, event: DispatchEvent) -> None:
        self._state_store.event_history.append(event)
        if len(self._state_store.event_history) > self._state_store.event_history_limit:
            self._state_store.event_history = self._state_store.event_history[
                -self._state_store.event_history_limit :
            ]

    def log_event(self, event: DispatchEvent) -> None:
        logger = self._state_store.logger
        if logger is None:
            return

        if event.event_type in {
            EventType.OPERATION_DISPATCHER_PAUSED,
            EventType.OPERATION_DISPATCHER_STOPPED,
        }:
            logger.warning("EVENT %s", event.event_type)
        elif event.event_type in {
            EventType.OPERATION_DISPATCHER_RESUMED,
        }:
            logger.info("EVENT %s", event.event_type)
        else:
            logger.debug(
                "EVENT %s for operation_id=%s",
                event.event_type,
                event.operation_id,
            )
