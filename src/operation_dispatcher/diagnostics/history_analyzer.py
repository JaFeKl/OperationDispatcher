from typing import Any
from operation_dispatcher.models import (
    History,
    ExecutionState,
    ExecutionOutcome,
    EventType,
)


class HistoryAnalyzer:
    def __init__(self, history: History):
        self.history = history

    def _dispatcher_uptime(self) -> float | None:
        """The total time (seconds) the Dispatcher has been running, meaning the time between OPERATION_DISPATCHER_STARTED and OPERATION_DISPATCHER_STOPPED events."""
        started_events = [
            event
            for event in self.history.events
            if event.event_type == EventType.OPERATION_DISPATCHER_STARTED
        ]
        stopped_events = [
            event
            for event in self.history.events
            if event.event_type == EventType.OPERATION_DISPATCHER_STOPPED
        ]
        # We need to pair start and stop events.
        # We need to check if the first event is a start or stop event to determine how to pair them.
        # If a stop event occurs first, we assume the dispatcher was already running at the start of th history.
        # If a start event occurs first, we assume the dispatcher was not running at the start of the history.
        # If the last event is a start event, we assume the dispatcher is still running at the end of the history.
        # If the last event is a stop event, we assume the dispatcher is not running at the end of the history.
        relevant_events = sorted(
            started_events + stopped_events,
            key=lambda event: event.created_at,
        )

        if not relevant_events:
            return None

        start = self.history.get_start()
        end = self.history.get_end()
        if start is None or end is None:
            return None
        uptime_seconds = 0.0

        running_since = None
        first_event = relevant_events[0]
        if first_event.event_type == EventType.OPERATION_DISPATCHER_STOPPED:
            running_since = start

        for event in relevant_events:
            if event.event_type == EventType.OPERATION_DISPATCHER_STARTED:
                if running_since is None:
                    running_since = event.created_at
                continue
            elif event.event_type == EventType.OPERATION_DISPATCHER_STOPPED:
                if running_since is not None:
                    interval_seconds = (
                        event.created_at - running_since
                    ).total_seconds()
                    if interval_seconds > 0:
                        uptime_seconds += interval_seconds
                    running_since = None

        if running_since is not None and end is not None:
            interval_seconds = (end - running_since).total_seconds()
            if interval_seconds > 0:
                uptime_seconds += interval_seconds

        return uptime_seconds

    def _dispatcher_paused_time(self) -> float | None:
        """The total time (seconds) the Dispatcher has been paused, meaning the time between OPERATION_DISPATCHER_PAUSED and OPERATION_DISPATCHER_RESUMED events."""
        paused_events = [
            event
            for event in self.history.events
            if event.event_type == EventType.OPERATION_DISPATCHER_PAUSED
        ]
        resumed_events = [
            event
            for event in self.history.events
            if event.event_type == EventType.OPERATION_DISPATCHER_RESUMED
        ]
        relevant_events = sorted(
            paused_events + resumed_events,
            key=lambda event: event.created_at,
        )

        if not relevant_events:
            return None

        start = self.history.get_start()
        end = self.history.get_end()
        if start is None or end is None:
            return None
        paused_time_seconds = 0.0

        paused_since = None
        first_event = relevant_events[0]
        if first_event.event_type == EventType.OPERATION_DISPATCHER_RESUMED:
            paused_since = start

        for event in relevant_events:
            if event.event_type == EventType.OPERATION_DISPATCHER_PAUSED:
                if paused_since is None:
                    paused_since = event.created_at
                continue
            elif event.event_type == EventType.OPERATION_DISPATCHER_RESUMED:
                if paused_since is not None:
                    interval_seconds = (event.created_at - paused_since).total_seconds()
                    if interval_seconds > 0:
                        paused_time_seconds += interval_seconds
                    paused_since = None

        if paused_since is not None and end is not None:
            interval_seconds = (end - paused_since).total_seconds()
            if interval_seconds > 0:
                paused_time_seconds += interval_seconds

        return paused_time_seconds

    def _dispatcher_uptime_percentage(self) -> float | None:
        """The percentage of time the Dispatcher has been running during the history window."""
        total_seconds = self.history.get_duration_seconds()
        if total_seconds is None or total_seconds == 0:
            return None
        uptime_seconds = self._dispatcher_uptime()
        return (uptime_seconds / total_seconds) * 100.0

    def _dispatcher_runtime(self) -> float | None:
        """The total time (seconds) the Dispatcher has been running and not paused during the history window."""
        uptime_seconds = self._dispatcher_uptime()
        paused_time_seconds = self._dispatcher_paused_time()
        if uptime_seconds is None or paused_time_seconds is None:
            return None
        runtime_seconds = uptime_seconds - paused_time_seconds
        return runtime_seconds

    def _dispatcher_runtime_percentage(self) -> float | None:
        """The percentage of time the Dispatcher has been running and not paused during the history window."""
        total_seconds = self.history.get_duration_seconds()
        if total_seconds is None or total_seconds == 0:
            return None
        runtime_seconds = self._dispatcher_runtime()
        if runtime_seconds is None:
            return None
        return (runtime_seconds / total_seconds) * 100.0

    def _number_of_operations(self) -> int:
        return len(self.history.get_operation_ids())

    def _number_of_completed_operations(self) -> int:
        if self.history.operations is not None:
            return sum(
                1
                for operation in self.history.operations
                if operation.state == ExecutionState.COMPLETED
            )
        else:
            # check events for completed operations
            return sum(
                1
                for event in self.history.events
                if event.event_type
                in [
                    EventType.OPERATION_COMPLETED,
                    EventType.OPERATION_FAILED,
                    EventType.OPERATION_CANCELLED,
                ]
            )

    def _number_of_successful_operations(self) -> int:
        if self.history.operations is not None:
            return sum(
                1
                for operation in self.history.operations
                if operation.outcome == ExecutionOutcome.SUCCESS
            )
        else:
            # check events for successful operations
            return sum(
                1
                for event in self.history.events
                if event.event_type == EventType.OPERATION_COMPLETED
            )

    def get_report(self) -> dict[str, Any]:
        return {
            "dispatcher_uptime_seconds": self._dispatcher_uptime(),
            "dispatcher_uptime_percentage": self._dispatcher_uptime_percentage(),
            "dispatcher_runtime_seconds": self._dispatcher_runtime(),
            "dispatcher_runtime_percentage": self._dispatcher_runtime_percentage(),
        }
