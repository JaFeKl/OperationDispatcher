from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from flask import jsonify, request
from flasgger import swag_from
from pydantic import ValidationError

from .models import (
    EventType,
    ExecutionState,
    OperationHistory,
    Operation,
)
from .operation_dispatcher import OperationDispatcher
from .runtime_controller import OperationDispatcherRuntimeController


class OperationDispatcherOpenAPI:
    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        default_operation_name: str = "payload",
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._resource_id = operation_dispatcher.dispatch_queue.resource_id
        self._default_operation_name = default_operation_name
        self._operation_openapi_schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": True,
        }
        self._operation_openapi_definitions: dict[str, Any] = {}
        self._runtime_controller = OperationDispatcherRuntimeController(
            operation_dispatcher=operation_dispatcher,
            startup_timeout_seconds=1.0,
            stop_join_timeout_seconds=2.0,
        )

    @property
    def runtime_controller(self) -> OperationDispatcherRuntimeController:
        return self._runtime_controller

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
        scheduled_operation: Operation,
    ) -> dict[str, Any]:
        execution = self._operation_dispatcher.get_execution(scheduled_operation.id)
        current_operation = self._operation_dispatcher.current_scheduled_operation
        return {
            "scheduled_operation": scheduled_operation.model_dump(mode="json"),
            "execution": (
                execution.model_dump(mode="json") if execution is not None else None
            ),
            "is_current_operation": (
                current_operation is not None
                and current_operation.id == scheduled_operation.id
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

        scheduled_operation = self._operation_dispatcher.get_scheduled_operation(
            parsed_operation_id
        )
        if scheduled_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        return self._build_operation_status_payload(scheduled_operation), 200

    def list_operations_response(
        self,
        state: str | None,
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        if state is not None:
            valid_states = {execution_state.value for execution_state in ExecutionState}
            if state not in valid_states:
                return self._error_response(
                    message=f"invalid state '{state}'",
                    code="invalid_state",
                    status_code=400,
                    details={"valid_states": sorted(valid_states)},
                )

        operation_by_id: dict[UUID, Operation] = {}
        current_operation = self._operation_dispatcher.current_scheduled_operation
        if current_operation is not None:
            operation_by_id[current_operation.id] = current_operation

        for scheduled_operation in self._operation_dispatcher.get_schedule():
            operation_by_id[scheduled_operation.id] = scheduled_operation

        history = self._operation_dispatcher.get_history(limit=None)
        for history_entry in history.entries:
            scheduled_operation = history_entry.scheduled_operation
            operation_by_id[scheduled_operation.id] = scheduled_operation

        payload: list[dict[str, Any]] = []
        for scheduled_operation in operation_by_id.values():
            operation_status = self._build_operation_status_payload(scheduled_operation)

            if state is not None:
                execution_payload = operation_status.get("execution")
                execution_state = (
                    execution_payload.get("state")
                    if isinstance(execution_payload, dict)
                    else None
                )
                if execution_state != state:
                    continue

            payload.append(operation_status)

        return payload, 200

    def get_operation_response(self, operation_id: str) -> tuple[dict[str, Any], int]:
        return self._resolve_operation_status_response(operation_id)

    def get_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_operation = self._operation_dispatcher.current_scheduled_operation
        current_execution = self._operation_dispatcher.current_execution
        if (
            current_operation is None
            or current_execution is None
            or current_execution.state is not ExecutionState.RUNNING
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

        operation_uuid = UUID(operation_status_payload["scheduled_operation"]["id"])
        events = [
            event.model_dump(mode="json")
            for event in self._operation_dispatcher.get_event_history()
            if event.operation_id == operation_uuid
        ]
        return events, 200

    def get_operations_history_response(
        self,
        limit: int | None,
    ) -> tuple[dict[str, Any], int]:
        resolved_limit = 50 if limit is None else limit
        if resolved_limit < 1:
            return self._error_response(
                message="limit must be greater than 0",
                code="invalid_limit",
                status_code=400,
            )
        if resolved_limit > 1000:
            return self._error_response(
                message="limit must be less than or equal to 1000",
                code="invalid_limit",
                status_code=400,
            )

        history = self._operation_dispatcher.get_history(limit=resolved_limit)
        return history.model_dump(mode="json"), 200

    def add_operation_response(
        self,
        request_payload: Any,
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

        scheduled_operations: list[Operation] = []
        for index, operation_request in enumerate(request_payload):
            if not isinstance(operation_request, dict):
                return self._error_response(
                    message="each list item must be a scheduled operation object",
                    code="invalid_operations",
                    status_code=400,
                    details={"index": index},
                )

            scheduled_operation_or_error, scheduled_operation_status = (
                self._build_scheduled_operation(operation_request)
            )
            if scheduled_operation_status != 201:
                scheduled_operation_or_error.setdefault("details", {})["index"] = index
                return scheduled_operation_or_error, scheduled_operation_status

            scheduled_operations.append(scheduled_operation_or_error)

        payload: list[dict[str, Any]] = []
        for scheduled_operation in scheduled_operations:
            try:
                self._operation_dispatcher.add(scheduled_operation)
            except (TypeError, ValueError) as error:
                return self._error_response(
                    message=str(error),
                    code="operation_rejected",
                    status_code=400,
                )
            payload.append(self._build_operation_status_payload(scheduled_operation))

        return payload, 201

    def _build_scheduled_operation(
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
            scheduled_operation = Operation.model_validate(
                {
                    "payload": payload,
                    "resource_id": self._resource_id,
                    "priority": request_payload.get("priority", 0),
                    "release_date": request_payload.get("release_date"),
                    "planned_duration": request_payload.get("planned_duration"),
                    "due_date": request_payload.get("due_date"),
                }
            )
        except (ValidationError, TypeError, ValueError) as error:
            return self._error_response(
                message=str(error),
                code="invalid_scheduled_operation",
                status_code=400,
            )

        return scheduled_operation, 201

    def cancel_operation_response(
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

        existing_operation = self._operation_dispatcher.get_scheduled_operation(
            parsed_operation_id
        )
        if existing_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )

        cancelled_operation = self._operation_dispatcher.cancel(parsed_operation_id)
        if cancelled_operation is None:
            return self._error_response(
                message="operation cancellation denied",
                code="operation_cancellation_denied",
                status_code=409,
            )

        return self._build_operation_status_payload(cancelled_operation), 200

    def pause_operation_response(
        self,
        operation_id: str,
    ) -> tuple[dict[str, Any], int]:
        operation_status_payload, operation_status_code = (
            self._resolve_operation_status_response(operation_id)
        )
        if operation_status_code != 200:
            return operation_status_payload, operation_status_code

        execution_payload = operation_status_payload.get("execution")
        if (
            not isinstance(execution_payload, dict)
            or execution_payload.get("state") is not ExecutionState.RUNNING.value
        ):
            return self._error_response(
                message="operation is not running",
                code="operation_not_running",
                status_code=409,
            )

        try:
            is_paused = self._operation_dispatcher.pause_current_operation()
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
    ) -> tuple[dict[str, Any], int]:
        operation_status_payload, operation_status_code = (
            self._resolve_operation_status_response(operation_id)
        )
        if operation_status_code != 200:
            return operation_status_payload, operation_status_code

        execution_payload = operation_status_payload.get("execution")
        if (
            not isinstance(execution_payload, dict)
            or execution_payload.get("state") is not ExecutionState.PAUSED.value
        ):
            return self._error_response(
                message="operation is not paused",
                code="operation_not_paused",
                status_code=409,
            )

        try:
            is_resumed = self._operation_dispatcher.resume_current_operation()
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
        return self._runtime_controller.get_state_payload(), 200

    def start_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.start()

    def stop_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.stop()

    def pause_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.pause()

    def resume_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        return self._runtime_controller.resume()

    def register_default_endpoints(self, app: Any) -> None:
        self.register_list_operations_endpoint(app)
        self.register_get_current_operation_endpoint(app)
        self.register_get_operations_history_endpoint(app)
        self.register_get_operation_endpoint(app)
        self.register_get_operation_events_endpoint(app)
        self.register_add_operation_endpoint(app)
        self.register_cancel_operation_endpoint(app)
        self.register_pause_operation_endpoint(app)
        self.register_resume_operation_endpoint(app)

        self.register_get_operation_dispatcher_state_endpoint(app)
        self.register_start_operation_dispatcher_endpoint(app)
        self.register_stop_operation_dispatcher_endpoint(app)
        self.register_pause_operation_dispatcher_endpoint(app)
        self.register_resume_operation_dispatcher_endpoint(app)

    def _register_json_endpoint(
        self,
        app: Any,
        *,
        method: str,
        route: str,
        endpoint_name: str,
        openapi_spec: dict[str, Any],
        response_handler: Callable[
            [], tuple[dict[str, Any] | list[dict[str, Any]], int]
        ],
    ) -> None:
        @app.route(route, methods=[method.upper()], endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def endpoint(**_route_kwargs: Any) -> tuple[Any, int]:
            payload, status_code = response_handler()
            return jsonify(payload), status_code

    @staticmethod
    def _parse_json_body() -> Any:
        return request.get_json(silent=True)

    def register_list_operations_endpoint(
        self,
        app: Any,
        route: str = "/operations",
        endpoint_name: str = "list_operations",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.list_operations_openapi_spec(),
            response_handler=lambda: self.list_operations_response(
                state=request.args.get("state"),
            ),
        )

    def register_get_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/current",
        endpoint_name: str = "get_current_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_current_operation_openapi_spec(),
            response_handler=self.get_current_operation_response,
        )

    def register_get_operations_history_endpoint(
        self,
        app: Any,
        route: str = "/operations/history",
        endpoint_name: str = "get_operations_history",
    ) -> None:
        def response_handler() -> tuple[dict[str, Any], int]:
            limit_value = request.args.get("limit")
            if limit_value is None:
                parsed_limit = None
            else:
                try:
                    parsed_limit = int(limit_value)
                except ValueError:
                    return self._error_response(
                        message="limit must be an integer",
                        code="invalid_limit",
                        status_code=400,
                    )

            return self.get_operations_history_response(parsed_limit)

        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_operations_history_openapi_spec(),
            response_handler=response_handler,
        )

    def register_get_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>",
        endpoint_name: str = "get_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_operation_openapi_spec(),
            response_handler=lambda: self.get_operation_response(
                operation_id=request.view_args.get("operation_id", "")
            ),
        )

    def register_get_operation_events_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/events",
        endpoint_name: str = "get_operation_events",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_operation_events_openapi_spec(),
            response_handler=lambda: self.get_operation_events_response(
                operation_id=request.view_args.get("operation_id", "")
            ),
        )

    def register_add_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/add",
        endpoint_name: str = "add_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.add_operation_openapi_spec(),
            response_handler=lambda: self.add_operation_response(
                self._parse_json_body()
            ),
        )

    def register_cancel_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/cancel",
        endpoint_name: str = "cancel_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.cancel_operation_openapi_spec(),
            response_handler=lambda: self.cancel_operation_response(
                operation_id=request.view_args.get("operation_id", "")
            ),
        )

    def register_pause_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/pause",
        endpoint_name: str = "pause_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.pause_operation_openapi_spec(),
            response_handler=lambda: self.pause_operation_response(
                operation_id=request.view_args.get("operation_id", "")
            ),
        )

    def register_resume_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/resume",
        endpoint_name: str = "resume_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.resume_operation_openapi_spec(),
            response_handler=lambda: self.resume_operation_response(
                operation_id=request.view_args.get("operation_id", "")
            ),
        )

    def register_get_operation_dispatcher_state_endpoint(
        self,
        app: Any,
        route: str = "/dispatcher",
        endpoint_name: str = "get_dispatcher_state",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_operation_dispatcher_state_openapi_spec(),
            response_handler=self.get_operation_dispatcher_state_response,
        )

    def register_start_operation_dispatcher_endpoint(
        self,
        app: Any,
        route: str = "/dispatcher/start",
        endpoint_name: str = "start_dispatcher",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.start_operation_dispatcher_openapi_spec(),
            response_handler=self.start_operation_dispatcher_response,
        )

    def register_stop_operation_dispatcher_endpoint(
        self,
        app: Any,
        route: str = "/dispatcher/stop",
        endpoint_name: str = "stop_dispatcher",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.stop_operation_dispatcher_openapi_spec(),
            response_handler=self.stop_operation_dispatcher_response,
        )

    def register_pause_operation_dispatcher_endpoint(
        self,
        app: Any,
        route: str = "/dispatcher/pause",
        endpoint_name: str = "pause_dispatcher",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.pause_operation_dispatcher_openapi_spec(),
            response_handler=self.pause_operation_dispatcher_response,
        )

    def register_resume_operation_dispatcher_endpoint(
        self,
        app: Any,
        route: str = "/dispatcher/resume",
        endpoint_name: str = "resume_dispatcher",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.resume_operation_dispatcher_openapi_spec(),
            response_handler=self.resume_operation_dispatcher_response,
        )

    @staticmethod
    def list_operations_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "state",
                    "in": "query",
                    "required": False,
                    "type": "string",
                    "description": "Execution state filter (QUEUED, RUNNING, PAUSED, COMPLETED, FAILED, CANCELLED).",
                },
            ],
            "responses": {
                200: {
                    "description": "Operations with current execution status.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationStatus"},
                    },
                },
                400: {
                    "description": "Invalid filter value.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                }
            ],
            "responses": {
                200: {
                    "description": "Operation status.",
                    "schema": {"$ref": "#/definitions/OperationStatus"},
                },
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def get_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current running operation status.",
                    "schema": {"$ref": "#/definitions/OperationStatus"},
                },
                404: {
                    "description": "No running operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_operation_events_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                }
            ],
            "responses": {
                200: {
                    "description": "Events for one operation.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/DispatchEvent"},
                    },
                },
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def get_operations_history_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "limit",
                    "in": "query",
                    "required": False,
                    "type": "integer",
                    "default": 50,
                    "minimum": 1,
                    "maximum": 1000,
                }
            ],
            "responses": {
                200: {
                    "description": "Most recent completed operations.",
                    "schema": {"$ref": "#/definitions/OperationHistory"},
                },
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def add_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "in": "body",
                    "name": "body",
                    "required": True,
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/AddOperationItem"},
                    },
                }
            ],
            "responses": {
                201: {
                    "description": "Operations added.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationStatus"},
                    },
                },
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def cancel_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                }
            ],
            "responses": {
                200: {"schema": {"$ref": "#/definitions/OperationStatus"}},
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                409: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def pause_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                }
            ],
            "responses": {
                200: {"schema": {"$ref": "#/definitions/OperationStatus"}},
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                409: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def resume_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                }
            ],
            "responses": {
                200: {"schema": {"$ref": "#/definitions/OperationStatus"}},
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                409: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def get_operation_dispatcher_state_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current dispatcher runtime state.",
                    "schema": {"$ref": "#/definitions/OperationDispatcherState"},
                }
            },
        }

    @staticmethod
    def start_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                202: {
                    "description": "Dispatcher start request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def stop_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                202: {
                    "description": "Dispatcher stop request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def pause_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Dispatcher pause request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def resume_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Dispatcher resume request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    def get_openapi_definitions(self) -> dict[str, Any]:
        definitions: dict[str, Any] = {
            "ErrorResponse": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"},
                    "message": {"type": "string"},
                    "code": {"type": "string"},
                    "details": {"type": "object", "additionalProperties": True},
                },
                "required": ["error", "message", "code"],
            },
            "Operation": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "payload": self._operation_openapi_schema,
                    "resource_id": {"type": "string"},
                    "priority": {"type": "integer"},
                    "release_date": {"type": "string", "format": "date-time"},
                    "planned_duration": {
                        "type": "integer",
                        "format": "int64",
                        "description": "Planned operation duration in milliseconds.",
                    },
                    "due_date": {"type": "string", "format": "date-time"},
                    "created_at": {"type": "string", "format": "date-time"},
                },
                "required": ["id", "payload", "resource_id", "priority", "created_at"],
            },
            "OperationExecution": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "operation_id": {"type": "string", "format": "uuid"},
                    "state": {"type": "string"},
                    "outcome": {"type": "string"},
                    "termination_reason": {"type": "string"},
                    "retry_count": {"type": "integer"},
                    "start_time": {"type": "string", "format": "date-time"},
                    "finish_time": {"type": "string", "format": "date-time"},
                },
                "required": [
                    "id",
                    "operation_id",
                    "state",
                    "outcome",
                    "termination_reason",
                    "retry_count",
                ],
            },
            "OperationStatus": {
                "type": "object",
                "properties": {
                    "scheduled_operation": {"$ref": "#/definitions/Operation"},
                    "execution": {
                        "oneOf": [
                            {"$ref": "#/definitions/OperationExecution"},
                            {"type": "null"},
                        ]
                    },
                    "is_current_operation": {"type": "boolean"},
                },
                "required": [
                    "scheduled_operation",
                    "execution",
                    "is_current_operation",
                ],
            },
            "DispatchEvent": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "execution_id": {"type": "string", "format": "uuid"},
                    "operation_id": {"type": "string", "format": "uuid"},
                    "event_type": {
                        "type": "string",
                        "enum": [event.value for event in EventType],
                    },
                    "created_at": {"type": "string", "format": "date-time"},
                    "payload": {"type": "object", "additionalProperties": True},
                },
                "required": [
                    "id",
                    "execution_id",
                    "operation_id",
                    "event_type",
                    "created_at",
                    "payload",
                ],
            },
            "OperationHistoryEntry": {
                "type": "object",
                "properties": {
                    "scheduled_operation": {"$ref": "#/definitions/Operation"},
                    "execution": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationExecution"},
                    },
                    "events": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/DispatchEvent"},
                    },
                },
                "required": ["scheduled_operation", "execution", "events"],
            },
            "OperationHistory": {
                "type": "object",
                "properties": {
                    "number_of_entries": {"type": "integer"},
                    "entries": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationHistoryEntry"},
                    },
                },
                "required": ["number_of_entries", "entries"],
            },
            "OperationDispatcherState": {
                "type": "object",
                "properties": {
                    "is_running": {"type": "boolean"},
                    "is_paused": {"type": "boolean"},
                    "queue_size": {"type": "integer"},
                    "current_operation": {
                        "oneOf": [
                            {"$ref": "#/definitions/Operation"},
                            {"type": "null"},
                        ]
                    },
                    "running_since": {"type": "string", "format": "date-time"},
                    "uptime_seconds": {"type": "number"},
                    "runtime_thread_alive": {"type": "boolean"},
                    "runtime_last_error": {"type": "string"},
                },
                "required": [
                    "is_running",
                    "is_paused",
                    "queue_size",
                    "current_operation",
                    "running_since",
                    "uptime_seconds",
                    "runtime_thread_alive",
                    "runtime_last_error",
                ],
            },
            "OperationDispatcherRuntimeActionResponse": {
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "state": {"$ref": "#/definitions/OperationDispatcherState"},
                },
                "required": ["message", "state"],
            },
            "AddOperationItem": {
                "type": "object",
                "properties": {
                    "payload": {**self._operation_openapi_schema, "example": {}},
                    "priority": {"type": "integer"},
                    "release_date": {"type": "string", "format": "date-time"},
                    "planned_duration": {
                        "type": "integer",
                        "format": "int64",
                        "description": "Planned operation duration in milliseconds.",
                    },
                    "due_date": {"type": "string", "format": "date-time"},
                },
                "required": ["payload"],
            },
        }

        definitions.update(self._operation_openapi_definitions)
        return definitions
