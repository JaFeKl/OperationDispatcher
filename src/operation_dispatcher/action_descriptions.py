from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActionDescription:
    description: str


OPENAPI_ACTION_DESCRIPTIONS: dict[str, ActionDescription] = {
    "list_operations": ActionDescription(
        description="Returns currently active operations (QUEUED, RUNNING, PAUSED)."
    ),
    "get_operation": ActionDescription(
        description="Returns a snapshot of an operation identified by its operation_id."
    ),
    "get_current_operation": ActionDescription(
        description="Returns a snapshot of the currently running operation; responds with not found when no operation is actively running."
    ),
    "get_operation_events": ActionDescription(
        description="Returns the ordered lifecycle event history emitted for the specified operation."
    ),
    "get_history": ActionDescription(
        description="Returns event and operation history matching the requested time window."
    ),
    "add_operation": ActionDescription(
        description="Adds one or more operations to the dispatcher queue and emits operation-added lifecycle events for accepted items."
    ),
    "cancel_operation": ActionDescription(
        description="Requests cancellation of a operation."
    ),
    "update_operation": ActionDescription(
        description="Applies partial updates to a queued and non-running operation."
    ),
    "pause_operation": ActionDescription(
        description="Requests pausing an operation. The operation must be currently running to be paused."
    ),
    "resume_operation": ActionDescription(
        description="Requests resuming of an paused operation."
    ),
    "get_dispatcher_state": ActionDescription(
        description="Returns the dispatcher runtime state information."
    ),
    "start_operation_dispatcher": ActionDescription(
        description="Starts the dispatcher so queued operations are automatically dispatched."
    ),
    "stop_operation_dispatcher": ActionDescription(
        description="Stops the dispatcher. No new operations are being dispatched until restarted. Running operations are not affected and will continue to run until completion or cancellation."
    ),
    "pause_operation_dispatcher": ActionDescription(
        description="Pauses the dispatcher. No new operations are being dispatched until resumed. "
    ),
    "resume_operation_dispatcher": ActionDescription(
        description="Resumes a paused dispatcher, allowing it to dispatch queued operations again."
    ),
}
