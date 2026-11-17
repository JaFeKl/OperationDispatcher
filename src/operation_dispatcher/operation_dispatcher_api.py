from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from .models import (
    CompleteOperationCommand,
    ExecutionState,
    Operation,
    TerminationReason,
)
from .operation_dispatcher import OperationDispatcher
from .runtime_controller import OperationDispatcherRuntimeController


class OperationDispatcherAPI:
    """Transport-independent API handlers returning a JSON payload and status code.

    HTTP and MCP adapters share these handlers, including validation and errors.
    The caller owns the dispatcher runtime; constructing this API does not start it.
    """

    _LISTABLE_OPERATION_STATES = frozenset(
        {
            ExecutionState.QUEUED.value,
            ExecutionState.RUNNING.value,
            ExecutionState.PAUSED.value,
        }
    )

    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        runtime_controller: OperationDispatcherRuntimeController | None = None,
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._resource_id = operation_dispatcher.resource_id
        self._runtime_controller = (
            runtime_controller
            if runtime_controller is not None
            else OperationDispatcherRuntimeController(operation_dispatcher)
        )

    def _get_runtime_state_payload(self) -> dict[str, Any]:
        return self._runtime_controller.get_state_payload()

    @staticmethod
    def _error_response(
        *,
        message: str,
        code: str,
        status_code: int,
        details: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        payload: dict[str, Any] = {
            "error": message,
            "message": message,
            "code": code,
        }
        if details:
            payload["details"] = details
        return payload, status_code

    def _build_operation_status_payload(
        self,
        operation: Operation,
    ) -> dict[str, Any]:
        current_operation = self._operation_dispatcher.current_operation
        return {
            "operation": operation.model_dump(mode="json"),
            "is_current_operation": (
                current_operation is not None and current_operation.id == operation.id
            ),
        }

    def _resolve_operation_status_response(
        self,
        operation_id: str,
    ) -> tuple[dict[str, Any], int]:
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation_id",
                status_code=400,
            )

        operation = self._operation_dispatcher.get_operation(parsed_operation_id)
        if operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        return self._build_operation_status_payload(operation), 200

    def list_operations_response(
        self,
        state: str | None = None,
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        if state is not None:
            valid_states = self._LISTABLE_OPERATION_STATES
            if state not in valid_states:
                return self._error_response(
                    message=f"invalid state '{state}'",
                    code="invalid_state",
                    status_code=400,
                    details={"valid_states": sorted(valid_states)},
                )

        operation_by_id: dict[UUID, Operation] = {}
        current_operation = self._operation_dispatcher.current_operation
        if current_operation is not None:
            operation_by_id[current_operation.id] = current_operation

        for operation in self._operation_dispatcher.get_schedule():
            operation_by_id[operation.id] = operation

        payload: list[dict[str, Any]] = []
        for operation in operation_by_id.values():
            if operation.state.value not in self._LISTABLE_OPERATION_STATES:
                continue

            operation_status = self._build_operation_status_payload(operation)

            if state is not None:
                if operation.state.value != state:
                    continue

            payload.append(operation_status)

        return payload, 200

    def get_operation_response(self, operation_id: str) -> tuple[dict[str, Any], int]:
        return self._resolve_operation_status_response(operation_id)

    def get_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_operation = self._operation_dispatcher.current_operation
        if (
            current_operation is None
            or current_operation.state is not ExecutionState.RUNNING
        ):
            return self._error_response(
                message="no running operation",
                code="no_running_operation",
                status_code=404,
            )
        return self._build_operation_status_payload(current_operation), 200

    def get_operation_events_response(
        self,
        operation_id: str,
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        operation_status_payload, operation_status_code = (
            self._resolve_operation_status_response(operation_id)
        )
        if operation_status_code != 200:
            return operation_status_payload, operation_status_code

        operation_uuid = UUID(operation_status_payload["operation"]["id"])
        events = [
            event.model_dump(mode="json")
            for event in self._operation_dispatcher.get_operation_events(operation_uuid)
        ]
        return events, 200

    def get_history_response(
        self,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        resolve_operations: bool = True,
        limit: int | None = None,
    ) -> tuple[dict[str, Any], int]:
        # Query dates without offsets use UTC, like operation timestamps.
        if from_time is not None and from_time.tzinfo is None:
            from_time = from_time.replace(tzinfo=timezone.utc)
        if to_time is not None and to_time.tzinfo is None:
            to_time = to_time.replace(tzinfo=timezone.utc)
        if from_time is not None and to_time is not None and from_time > to_time:
            return self._error_response(
                message="from_time must be less than or equal to to_time",
                code="invalid_time_range",
                status_code=400,
            )
        max_limit = self._operation_dispatcher.event_history_limit
        resolved_limit = min(50, max_limit) if limit is None else limit
        if resolved_limit < 1:
            return self._error_response(
                message="limit must be greater than 0",
                code="invalid_limit",
                status_code=400,
            )
        if resolved_limit > max_limit:
            return self._error_response(
                message=f"limit must be less than or equal to {max_limit}",
                code="invalid_limit",
                status_code=400,
            )

        history = self._operation_dispatcher.get_history(
            from_time, to_time, resolve_operations, limit=resolved_limit
        )
        return history.model_dump(mode="json"), 200

    def add_operation_response(
        self,
        request_payload: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        if not isinstance(request_payload, list):
            return self._error_response(
                message="request body must be a list of scheduled operations",
                code="invalid_operations",
                status_code=400,
            )
        if len(request_payload) == 0:
            return self._error_response(
                message="operations list must not be empty",
                code="missing_operations",
                status_code=400,
            )

        operations: list[Operation] = []
        for index, operation_request in enumerate(request_payload):
            if not isinstance(operation_request, dict):
                return self._error_response(
                    message="each list item must be a scheduled operation object",
                    code="invalid_operations",
                    status_code=400,
                    details={"index": index},
                )

            operation_or_error, operation_status = self._build_operation(
                operation_request
            )
            if operation_status != 201:
                operation_or_error.setdefault("details", {})["index"] = index
                return operation_or_error, operation_status

            operations.append(operation_or_error)

        payload: list[dict[str, Any]] = []
        for operation in operations:
            try:
                self._operation_dispatcher.add_operation(operation)
            except (TypeError, ValueError) as error:
                return self._error_response(
                    message=str(error),
                    code="operation_rejected",
                    status_code=400,
                )
            payload.append(self._build_operation_status_payload(operation))

        return payload, 201

    def _build_operation(
        self,
        request_payload: dict[str, Any],
    ) -> tuple[Operation | dict[str, Any], int]:
        if "resource_id" in request_payload:
            return self._error_response(
                message="resource_id must not be provided; it is assigned by the dispatcher resource",
                code="resource_id_not_allowed",
                status_code=400,
            )

        payload = request_payload.get("payload")
        if payload is None:
            return self._error_response(
                message="payload is required",
                code="missing_payload",
                status_code=400,
            )

        if not isinstance(payload, dict):
            return self._error_response(
                message="payload must be an object",
                code="invalid_payload",
                status_code=400,
            )

        try:
            operation = Operation.model_validate(
                {
                    "job_id": request_payload.get("job_id"),
                    "payload": payload,
                    "resource_id": self._resource_id,
                    "priority": request_payload.get("priority", 0),
                    "release_date": request_payload.get("release_date"),
                    "planned_duration": request_payload.get("planned_duration"),
                    "due_date": request_payload.get("due_date"),
                    "dependencies": request_payload.get("dependencies", []),
                }
            )
        except (ValidationError, TypeError, ValueError) as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation",
                status_code=400,
            )

        return operation, 201

    def _parse_meta_data(
        self,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int]:
        meta_data = payload.get("meta_data")
        if meta_data is None:
            return None, None, 200
        if not isinstance(meta_data, dict):
            error_payload, status_code = self._error_response(
                message="meta_data must be an object",
                code="invalid_meta_data",
                status_code=400,
            )
            return None, error_payload, status_code
        return dict(meta_data), None, 200

    def _parse_termination_reason(
        self,
        payload: dict[str, Any],
    ) -> tuple[TerminationReason, dict[str, Any] | None, int]:
        raw_termination_reason = payload.get("termination_reason")
        if raw_termination_reason is None:
            return TerminationReason.INTERNAL_ERROR, None, 200

        if isinstance(raw_termination_reason, TerminationReason):
            return raw_termination_reason, None, 200

        try:
            return TerminationReason(str(raw_termination_reason)), None, 200
        except ValueError:
            error_payload, status_code = self._error_response(
                message="invalid termination_reason",
                code="invalid_termination_reason",
                status_code=400,
                details={
                    "valid_termination_reasons": [
                        reason.value for reason in TerminationReason
                    ]
                },
            )
            return TerminationReason.INTERNAL_ERROR, error_payload, status_code

    def complete_operation_response(
        self,
        operation_id: str,
        command_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        try:
            command = CompleteOperationCommand.model_validate(command_payload)
        except ValidationError as error:
            return self._error_response(
                message=str(error), code="invalid_completion_command", status_code=400
            )
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return self._error_response(
                message=str(error), code="invalid_operation_id", status_code=400
            )
        try:
            operation = self._operation_dispatcher.request_operation_completion(
                parsed_operation_id,
                command.outcome,
                termination_reason=command.termination_reason,
                meta_data=command.meta_data,
            )
        except LookupError:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )
        except RuntimeError as error:
            return self._error_response(
                message=str(error),
                code="operation_completion_conflict",
                status_code=409,
            )
        if operation is None:
            return self._error_response(
                message="operation completion denied",
                code="operation_completion_denied",
                status_code=409,
            )
        return self._build_operation_status_payload(operation), 200

    def cancel_operation_response(
        self,
        operation_id: str,
        command_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        if command_payload is None:
            resolved_command_payload: dict[str, Any] = {}
        elif isinstance(command_payload, dict):
            resolved_command_payload = command_payload
        else:
            return self._error_response(
                message="request body must be an object",
                code="invalid_request_body",
                status_code=400,
            )

        meta_data, meta_data_error, meta_data_status = self._parse_meta_data(
            resolved_command_payload
        )
        if meta_data_error is not None:
            return meta_data_error, meta_data_status

        (
            termination_reason,
            termination_reason_error,
            termination_reason_status,
        ) = self._parse_termination_reason(resolved_command_payload)
        if termination_reason_error is not None:
            return termination_reason_error, termination_reason_status

        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation_id",
                status_code=400,
            )

        existing_operation = self._operation_dispatcher.get_operation(
            parsed_operation_id
        )
        if existing_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        try:
            cancelled_operation = self._operation_dispatcher.cancel_operation(
                parsed_operation_id,
                termination_reason=termination_reason,
                meta_data=meta_data,
            )
        except RuntimeError as error:
            return self._error_response(
                message=str(error),
                code="operation_cancellation_conflict",
                status_code=409,
            )
        if cancelled_operation is None:
            return self._error_response(
                message="operation cancellation denied",
                code="operation_cancellation_denied",
                status_code=409,
            )

        return self._build_operation_status_payload(cancelled_operation), 200

    def update_operation_response(
        self,
        operation_id: str,
        updates_payload: dict[str, Any],
    ) -> tuple[dict[str, Any], int]:
        if not isinstance(updates_payload, dict):
            return self._error_response(
                message="request body must be an object of updates",
                code="invalid_updates",
                status_code=400,
            )

        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation_id",
                status_code=400,
            )

        existing_operation = self._operation_dispatcher.get_operation(
            parsed_operation_id
        )
        if existing_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        try:
            updated_operation = self._operation_dispatcher.update_operation(
                parsed_operation_id,
                updates_payload,
            )
        except RuntimeError as error:
            if str(error) == "cannot update a running operation":
                return self._error_response(
                    message="cannot update a running operation",
                    code="operation_update_denied",
                    status_code=409,
                )
            return self._error_response(
                message=str(error),
                code="operation_update_failed",
                status_code=409,
            )
        except (TypeError, ValueError, ValidationError) as error:
            return self._error_response(
                message=str(error),
                code="invalid_updates",
                status_code=400,
            )

        if updated_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        return self._build_operation_status_payload(updated_operation), 200

    def pause_operation_response(
        self,
        operation_id: str,
        command_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        if command_payload is None:
            resolved_command_payload: dict[str, Any] = {}
        elif isinstance(command_payload, dict):
            resolved_command_payload = command_payload
        else:
            return self._error_response(
                message="request body must be an object",
                code="invalid_request_body",
                status_code=400,
            )

        meta_data, meta_data_error, meta_data_status = self._parse_meta_data(
            resolved_command_payload
        )
        if meta_data_error is not None:
            return meta_data_error, meta_data_status

        operation_status_payload, operation_status_code = (
            self._resolve_operation_status_response(operation_id)
        )
        if operation_status_code != 200:
            return operation_status_payload, operation_status_code

        operation_payload = operation_status_payload.get("operation")
        if (
            not isinstance(operation_payload, dict)
            or operation_payload.get("state") != ExecutionState.RUNNING.value
        ):
            return self._error_response(
                message="operation is not running",
                code="operation_not_running",
                status_code=409,
            )

        try:
            operation_uuid = UUID(operation_status_payload["operation"]["id"])
            is_paused = self._operation_dispatcher.pause_operation(
                operation_uuid,
                meta_data=meta_data,
            )
        except RuntimeError as error:
            if str(error) == "current operation is not running":
                return self._error_response(
                    message="operation is not running",
                    code="operation_not_running",
                    status_code=409,
                )
            return self._error_response(
                message="operation pause failed",
                code="operation_pause_failed",
                status_code=409,
            )

        if not is_paused:
            return self._error_response(
                message="operation pause denied",
                code="operation_pause_denied",
                status_code=409,
            )

        return self._resolve_operation_status_response(operation_id)

    def resume_operation_response(
        self,
        operation_id: str,
        command_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        if command_payload is None:
            resolved_command_payload: dict[str, Any] = {}
        elif isinstance(command_payload, dict):
            resolved_command_payload = command_payload
        else:
            return self._error_response(
                message="request body must be an object",
                code="invalid_request_body",
                status_code=400,
            )

        meta_data, meta_data_error, meta_data_status = self._parse_meta_data(
            resolved_command_payload
        )
        if meta_data_error is not None:
            return meta_data_error, meta_data_status

        operation_status_payload, operation_status_code = (
            self._resolve_operation_status_response(operation_id)
        )
        if operation_status_code != 200:
            return operation_status_payload, operation_status_code

        operation_payload = operation_status_payload.get("operation")
        if (
            not isinstance(operation_payload, dict)
            or operation_payload.get("state") != ExecutionState.PAUSED.value
        ):
            return self._error_response(
                message="operation is not paused",
                code="operation_not_paused",
                status_code=409,
            )

        try:
            operation_uuid = UUID(operation_status_payload["operation"]["id"])
            is_resumed = self._operation_dispatcher.resume_operation(
                operation_uuid,
                meta_data=meta_data,
            )
        except RuntimeError as error:
            if str(error) == "current operation is not paused":
                return self._error_response(
                    message="operation is not paused",
                    code="operation_not_paused",
                    status_code=409,
                )
            return self._error_response(
                message="operation resume failed",
                code="operation_resume_failed",
                status_code=409,
            )

        if not is_resumed:
            return self._error_response(
                message="operation resume denied",
                code="operation_resume_denied",
                status_code=409,
            )

        return self._resolve_operation_status_response(operation_id)

    def get_operation_dispatcher_state_response(self) -> tuple[dict[str, Any], int]:
        return self._get_runtime_state_payload(), 200

    def start_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.start()

    def stop_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.stop()

    def pause_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.pause()

    def resume_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.resume()
