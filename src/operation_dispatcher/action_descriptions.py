from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActionDescription:
    description: str


ACTION_DESCRIPTIONS: dict[str, ActionDescription] = {
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
        description="Returns event and operation history matching the requested time window. The limit must be between 1 and the dispatcher's configured event_history_limit; the default is the smaller of 50 and that limit."
    ),
    "add_operation": ActionDescription(
        description="Adds one or more operations to the dispatcher queue and emits operation-added lifecycle events for accepted items."
    ),
    "cancel_operation": ActionDescription(
        description="Requests cancellation of a operation."
    ),
    "complete_operation": ActionDescription(
        description="Requests application approval to complete the current RUNNING or PAUSED operation with SUCCESS or FAILURE. Requires a configured approval callback."
    ),
    "update_operation": ActionDescription(
        description="Updates payload or scheduling fields on a QUEUED or PAUSED operation. Lifecycle fields and completed operations cannot be updated."
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
        description="Pauses dispatching new operations until resumed. The active operation's state is unchanged; use the operation pause request to pause active work."
    ),
    "resume_operation_dispatcher": ActionDescription(
        description="Resumes a paused dispatcher, allowing it to dispatch queued operations again."
    ),
}

# Preserve the original public name for OpenAPI integrations.
OPENAPI_ACTION_DESCRIPTIONS = ACTION_DESCRIPTIONS
