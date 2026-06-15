from __future__ import annotations
from typing import Optional

from operation_dispatcher.models import DispatchEvent, History
from datetime import datetime
from .state_store import DispatcherStateStore


class DispatcherHistoryService:
    def __init__(self, state_store: DispatcherStateStore) -> None:
        self._state_store = state_store

    def get_history(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        resolve_operations: bool = False,
        limit: Optional[int] = None,
    ) -> History:
        """Return dispatcher history from in-memory events and optional callback override.

        Behavior:
        - Without an explicit time window (`from_time` and `to_time` are both `None`),
          `limit` returns the most recent events.
        - With an explicit time window (`from_time` and/or `to_time`), events are
          filtered in chronological order and `limit` returns the most recent matching
          events within that filtered window.
        - When `resolve_operations=True`, operations referenced by the selected events
          are included in the returned `History`.
        - If `on_history_callback` is configured, it receives
          `(from_time, to_time, resolve_operations, limit, in_memory_history)` and may
          return a `History` to override the in-memory result. Returning `None` keeps
          the in-memory result.
        """
        in_memory_history = self._get_in_memory_history(
            from_time, to_time, resolve_operations, limit
        )
        if self._state_store.on_history_callback is None:
            return in_memory_history

        callback_result = self._state_store.on_history_callback(
            from_time, to_time, resolve_operations, limit, in_memory_history
        )
        if callback_result is None:
            return in_memory_history
        if isinstance(callback_result, History):
            return callback_result
        else:
            raise ValueError(
                "Invalid return type from on_history_callback, expected History or None"
            )

    def _get_in_memory_history(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        resolve_operations: bool = False,
        limit: Optional[int] = None,
    ) -> History:
        """
        Get history from in-memory state store, applying time filtering and limit.
        """
        filtered_events: list[DispatchEvent] = []
        for event in self._state_store.event_history:
            if from_time is not None and event.created_at < from_time:
                continue
            if to_time is not None and event.created_at > to_time:
                continue
            filtered_events.append(event)

        if limit is not None:
            if limit <= 0:
                filtered_events = []
            else:
                filtered_events = filtered_events[-limit:]

        operations = []
        if resolve_operations is True:
            operation_ids = {
                event.operation_id
                for event in filtered_events
                if event.operation_id is not None
            }
            for operation_id in operation_ids:
                operation = self._state_store.dispatch_queue.get(operation_id)
                if operation is not None:
                    operations.append(operation)

        return History(
            resource_id=self._state_store.dispatch_queue.resource_id,
            window={
                "start": from_time,
                "end": to_time,
            },
            events=filtered_events,
            operations=operations if resolve_operations else None,
        )
