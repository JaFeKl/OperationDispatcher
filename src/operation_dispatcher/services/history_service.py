from __future__ import annotations

from operation_dispatcher.models import DispatchEvent, History, HistoryRecord

from .state_store import DispatcherStateStore


class DispatcherHistoryService:
    def __init__(self, state_store: DispatcherStateStore) -> None:
        self._state_store = state_store

    def get_event_history(self, limit: int | None = None) -> list[DispatchEvent]:
        if limit is None:
            return list(self._state_store.event_history)
        return list(self._state_store.event_history[-limit:])

    def get_history(self, limit: int | None = None) -> History:
        in_memory_history = self._get_in_memory_history(limit)
        if self._state_store.on_history_callback is None:
            return in_memory_history

        callback_result = self._state_store.on_history_callback(limit, in_memory_history)
        if callback_result is None:
            return in_memory_history
        if isinstance(callback_result, History):
            return callback_result

        return History(
            num_records=len(callback_result),
            records=callback_result,
        )

    def _get_in_memory_history(self, limit: int | None) -> History:
        history_operations = self._state_store.dispatch_queue.history(limit=limit)
        history_records: list[HistoryRecord] = []

        for operation in history_operations:
            operation_events = list(
                self._state_store.events_by_operation_id.get(operation.id, [])
            )
            history_records.append(
                HistoryRecord(
                    operation=operation,
                    events=operation_events,
                )
            )

        return History(
            num_records=len(history_records),
            records=history_records,
        )
