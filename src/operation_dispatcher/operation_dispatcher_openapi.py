from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from flask import jsonify, request
from flasgger import swag_from

from .models import (
    DEFAULT_EVENT_HISTORY_LIMIT,
    DependencyType,
    EventType,
    TerminationReason,
)
from .action_descriptions import OPENAPI_ACTION_DESCRIPTIONS
from .operation_dispatcher import OperationDispatcher
from .operation_dispatcher_api import OperationDispatcherAPI
from .runtime_controller import OperationDispatcherRuntimeController


class OperationDispatcherOpenAPI(OperationDispatcherAPI):
    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        default_operation_payload: Optional[dict[str, Any]] = None,
        runtime_controller: Optional[
            OperationDispatcherRuntimeController | None
        ] = None,
    ) -> None:
        super().__init__(operation_dispatcher, runtime_controller=runtime_controller)
        self._default_operation_payload = (
            dict(default_operation_payload)
            if default_operation_payload is not None
            else {}
        )
        self._operation_openapi_definitions: dict[str, Any] = {}

    def _build_payload_property_schema(self) -> dict[str, Any]:
        payload_schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": True,
        }
        if self._default_operation_payload:
            payload_schema["example"] = dict(self._default_operation_payload)
        return payload_schema

    def register_default_endpoints(self, app: Any) -> None:
        self.register_list_operations_endpoint(app)
        self.register_get_current_operation_endpoint(app)
        self.register_get_history_endpoint(app)
        self.register_get_operation_endpoint(app)
        self.register_get_operation_events_endpoint(app)
        self.register_add_operation_endpoint(app)
        self.register_update_operation_endpoint(app)
        self.register_cancel_operation_endpoint(app)
        self.register_complete_operation_endpoint(app)
        self.register_pause_operation_endpoint(app)
        self.register_resume_operation_endpoint(app)

        self.register_get_operation_dispatcher_state_endpoint(app)
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

    def register_get_history_endpoint(
        self,
        app: Any,
        route: str = "/operations/history",
        endpoint_name: str = "get_history",
    ) -> None:
        def response_handler() -> tuple[dict[str, Any], int]:
            def parse_datetime_query_param(
                value: str,
                *,
                param_name: str,
            ) -> tuple[datetime | None, dict[str, Any] | None, int]:
                try:
                    normalized_value = value.replace("Z", "+00:00")
                    return datetime.fromisoformat(normalized_value), None, 200
                except ValueError:
                    error_payload, status_code = self._error_response(
                        message=f"{param_name} must be a valid ISO-8601 datetime",
                        code=f"invalid_{param_name}",
                        status_code=400,
                    )
                    return None, error_payload, status_code

            def parse_bool_query_param(
                value: str,
                *,
                param_name: str,
            ) -> tuple[bool | None, dict[str, Any] | None, int]:
                normalized = value.strip().lower()
                if normalized in {"1", "true", "yes", "on"}:
                    return True, None, 200
                if normalized in {"0", "false", "no", "off"}:
                    return False, None, 200
                error_payload, status_code = self._error_response(
                    message=f"{param_name} must be a boolean",
                    code=f"invalid_{param_name}",
                    status_code=400,
                )
                return None, error_payload, status_code

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

            from_time: datetime | None = None
            from_time_value = request.args.get("from_time")
            if from_time_value is not None:
                parsed_from_time, from_time_error, from_time_status = (
                    parse_datetime_query_param(
                        from_time_value,
                        param_name="from_time",
                    )
                )
                if from_time_error is not None:
                    return from_time_error, from_time_status
                from_time = parsed_from_time

            to_time: datetime | None = None
            to_time_value = request.args.get("to_time")
            if to_time_value is not None:
                parsed_to_time, to_time_error, to_time_status = (
                    parse_datetime_query_param(
                        to_time_value,
                        param_name="to_time",
                    )
                )
                if to_time_error is not None:
                    return to_time_error, to_time_status
                to_time = parsed_to_time

            resolve_operations = True
            resolve_operations_value = request.args.get("resolve_operations")
            if resolve_operations_value is not None:
                (
                    parsed_resolve_operations,
                    resolve_operations_error,
                    resolve_operations_status,
                ) = parse_bool_query_param(
                    resolve_operations_value,
                    param_name="resolve_operations",
                )
                if resolve_operations_error is not None:
                    return resolve_operations_error, resolve_operations_status
                resolve_operations = bool(parsed_resolve_operations)

            return self.get_history_response(
                from_time=from_time,
                to_time=to_time,
                resolve_operations=resolve_operations,
                limit=parsed_limit,
            )

        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_history_openapi_spec(
                self._operation_dispatcher.event_history_limit
            ),
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

    def register_complete_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/complete",
        endpoint_name: str = "complete_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.complete_operation_openapi_spec(),
            response_handler=lambda: self.complete_operation_response(
                operation_id=request.view_args.get("operation_id", ""),
                command_payload=self._parse_json_body(),
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
                operation_id=request.view_args.get("operation_id", ""),
                command_payload=self._parse_json_body(),
            ),
        )

    def register_update_operation_endpoint(
        self,
        app: Any,
        route: str = "/operations/<operation_id>/update",
        endpoint_name: str = "update_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.update_operation_openapi_spec(),
            response_handler=lambda: self.update_operation_response(
                operation_id=request.view_args.get("operation_id", ""),
                updates_payload=self._parse_json_body(),
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
                operation_id=request.view_args.get("operation_id", ""),
                command_payload=self._parse_json_body(),
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
                operation_id=request.view_args.get("operation_id", ""),
                command_payload=self._parse_json_body(),
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
            "description": OPENAPI_ACTION_DESCRIPTIONS["list_operations"].description,
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "state",
                    "in": "query",
                    "required": False,
                    "type": "string",
                    "description": "Execution state filter (QUEUED, RUNNING, PAUSED). Terminal states are available via /operations/history.",
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
            "description": OPENAPI_ACTION_DESCRIPTIONS["get_operation"].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "get_current_operation"
            ].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "get_operation_events"
            ].description,
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
    def get_history_openapi_spec(
        event_history_limit: int = DEFAULT_EVENT_HISTORY_LIMIT,
    ) -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "description": OPENAPI_ACTION_DESCRIPTIONS["get_history"].description,
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "from_time",
                    "in": "query",
                    "required": False,
                    "type": "string",
                    "format": "date-time",
                },
                {
                    "name": "to_time",
                    "in": "query",
                    "required": False,
                    "type": "string",
                    "format": "date-time",
                },
                {
                    "name": "resolve_operations",
                    "in": "query",
                    "required": False,
                    "type": "boolean",
                    "default": True,
                },
                {
                    "name": "limit",
                    "in": "query",
                    "required": False,
                    "type": "integer",
                    "default": min(50, event_history_limit),
                    "minimum": 1,
                    "maximum": event_history_limit,
                },
            ],
            "responses": {
                200: {
                    "description": "History of events and operations in the given time window.",
                    "schema": {"$ref": "#/definitions/History"},
                },
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def add_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "description": OPENAPI_ACTION_DESCRIPTIONS["add_operation"].description,
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
    def complete_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "complete_operation"
            ].description,
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                },
                {
                    "in": "body",
                    "name": "body",
                    "required": True,
                    "schema": {"$ref": "#/definitions/CompleteOperationCommand"},
                },
            ],
            "responses": {
                200: {"schema": {"$ref": "#/definitions/OperationStatus"}},
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                409: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def cancel_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "description": OPENAPI_ACTION_DESCRIPTIONS["cancel_operation"].description,
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                },
                {
                    "in": "body",
                    "name": "body",
                    "required": False,
                    "schema": {
                        "$ref": "#/definitions/CancelOperationCommand",
                    },
                },
            ],
            "responses": {
                200: {"schema": {"$ref": "#/definitions/OperationStatus"}},
                400: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                404: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
                409: {"schema": {"$ref": "#/definitions/ErrorResponse"}},
            },
        }

    @staticmethod
    def update_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operations"],
            "description": OPENAPI_ACTION_DESCRIPTIONS["update_operation"].description,
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                },
                {
                    "in": "body",
                    "name": "body",
                    "required": True,
                    "schema": {
                        "$ref": "#/definitions/UpdateOperationItem",
                    },
                },
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
            "description": OPENAPI_ACTION_DESCRIPTIONS["pause_operation"].description,
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                },
                {
                    "in": "body",
                    "name": "body",
                    "required": False,
                    "schema": {
                        "$ref": "#/definitions/LifecycleCommand",
                    },
                },
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
            "description": OPENAPI_ACTION_DESCRIPTIONS["resume_operation"].description,
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "name": "operation_id",
                    "in": "path",
                    "required": True,
                    "type": "string",
                    "format": "uuid",
                },
                {
                    "in": "body",
                    "name": "body",
                    "required": False,
                    "schema": {
                        "$ref": "#/definitions/LifecycleCommand",
                    },
                },
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "get_dispatcher_state"
            ].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "start_operation_dispatcher"
            ].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "stop_operation_dispatcher"
            ].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "pause_operation_dispatcher"
            ].description,
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
            "description": OPENAPI_ACTION_DESCRIPTIONS[
                "resume_operation_dispatcher"
            ].description,
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
                    "job_id": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "null"},
                        ]
                    },
                    "payload": self._build_payload_property_schema(),
                    "resource_id": {"type": "string"},
                    "priority": {"type": "integer"},
                    "release_date": {"type": "string", "format": "date-time"},
                    "planned_duration": {
                        "type": "integer",
                        "format": "int64",
                        "description": "Planned operation duration in milliseconds.",
                    },
                    "due_date": {"type": "string", "format": "date-time"},
                    "dependencies": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationDependency"},
                    },
                    "state": {"type": "string"},
                    "outcome": {"type": "string"},
                    "termination_reason": {"type": "string"},
                    "retry_count": {"type": "integer"},
                    "start_time": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"},
                            {"type": "null"},
                        ]
                    },
                    "finish_time": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"},
                            {"type": "null"},
                        ]
                    },
                    "created_at": {"type": "string", "format": "date-time"},
                },
                "required": [
                    "id",
                    "payload",
                    "resource_id",
                    "priority",
                    "dependencies",
                    "state",
                    "outcome",
                    "termination_reason",
                    "retry_count",
                    "created_at",
                ],
            },
            "OperationStatus": {
                "type": "object",
                "properties": {
                    "operation": {"$ref": "#/definitions/Operation"},
                    "is_current_operation": {"type": "boolean"},
                },
                "required": [
                    "operation",
                    "is_current_operation",
                ],
            },
            "OperationDependency": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "depends_on_operation_id": {
                        "type": "string",
                        "format": "uuid",
                    },
                    "dependency_type": {
                        "type": "string",
                        "enum": [dependency.value for dependency in DependencyType],
                    },
                    "created_at": {"type": "string", "format": "date-time"},
                },
                "required": [
                    "id",
                    "depends_on_operation_id",
                    "dependency_type",
                    "created_at",
                ],
            },
            "ChangeRecord": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "old_value": {},
                    "new_value": {},
                },
                "required": ["field", "old_value", "new_value"],
            },
            "DispatchEvent": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "resource_id": {"type": "string"},
                    "operation_id": {
                        "oneOf": [
                            {"type": "string", "format": "uuid"},
                            {"type": "null"},
                        ]
                    },
                    "event_type": {
                        "type": "string",
                        "enum": [event.value for event in EventType],
                    },
                    "created_at": {"type": "string", "format": "date-time"},
                    "changes": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/ChangeRecord"},
                    },
                    "meta_data": {
                        "type": "object",
                        "additionalProperties": True,
                        "default": {},
                        "example": {},
                    },
                },
                "required": [
                    "id",
                    "resource_id",
                    "operation_id",
                    "event_type",
                    "created_at",
                    "changes",
                    "meta_data",
                ],
            },
            "TimeWindow": {
                "type": "object",
                "properties": {
                    "start": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"},
                            {"type": "null"},
                        ]
                    },
                    "end": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"},
                            {"type": "null"},
                        ]
                    },
                },
                "required": ["start", "end"],
            },
            "History": {
                "type": "object",
                "properties": {
                    "resource_id": {"type": "string"},
                    "window": {"$ref": "#/definitions/TimeWindow"},
                    "events": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/DispatchEvent"},
                    },
                    "operations": {
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {"$ref": "#/definitions/Operation"},
                            },
                            {"type": "null"},
                        ]
                    },
                    "created_at": {"type": "string", "format": "date-time"},
                },
                "required": ["resource_id", "window", "events", "created_at"],
            },
            "OperationDispatcherState": {
                "type": "object",
                "properties": {
                    "resource_id": {"type": "string"},
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
                    "resource_id",
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
                    "job_id": {
                        "type": "string",
                        "description": "Optional business-level operation identifier independent from dependency links.",
                    },
                    "payload": self._build_payload_property_schema(),
                    "priority": {"type": "integer"},
                    "release_date": {"type": "string", "format": "date-time"},
                    "planned_duration": {
                        "type": "integer",
                        "format": "int64",
                        "description": "Planned operation duration in milliseconds.",
                    },
                    "due_date": {"type": "string", "format": "date-time"},
                    "dependencies": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "depends_on_operation_id": {
                                    "type": "string",
                                    "format": "uuid",
                                },
                                "dependency_type": {
                                    "type": "string",
                                    "enum": [
                                        dependency.value
                                        for dependency in DependencyType
                                    ],
                                },
                            },
                            "required": [
                                "depends_on_operation_id",
                                "dependency_type",
                            ],
                        },
                        "description": "Optional list of dependency descriptors evaluated by dependency_type and depends_on_operation_id.",
                    },
                },
                "required": ["payload"],
            },
            "UpdateOperationItem": {
                "type": "object",
                "additionalProperties": False,
                "description": "Partial payload or scheduling updates. Only QUEUED and PAUSED operations may be changed; lifecycle fields are read-only.",
                "example": {
                    "priority": 2,
                    "payload": {"task": "updated"},
                },
            },
            "CompleteOperationCommand": {
                "type": "object",
                "required": ["outcome"],
                "properties": {
                    "outcome": {"type": "string", "enum": ["SUCCESS", "FAILURE"]},
                    "termination_reason": {
                        "type": "string",
                        "enum": [reason.value for reason in TerminationReason],
                        "x-nullable": True,
                        "description": "Defaults to NONE for SUCCESS (the only allowed reason), or INTERNAL_ERROR for FAILURE.",
                    },
                    "meta_data": {
                        "type": "object",
                        "additionalProperties": True,
                        "default": {},
                    },
                },
                "example": {"outcome": "SUCCESS", "meta_data": {}},
            },
            "LifecycleCommand": {
                "type": "object",
                "properties": {
                    "meta_data": {
                        "type": "object",
                        "additionalProperties": True,
                        "default": {},
                        "example": {},
                    },
                },
                "example": {"meta_data": {}},
            },
            "CancelOperationCommand": {
                "type": "object",
                "properties": {
                    "meta_data": {
                        "type": "object",
                        "additionalProperties": True,
                        "default": {},
                        "example": {},
                    },
                    "termination_reason": {
                        "type": "string",
                        "enum": [reason.value for reason in TerminationReason],
                        "default": TerminationReason.INTERNAL_ERROR.value,
                    },
                },
                "example": {
                    "meta_data": {},
                    "termination_reason": TerminationReason.INTERNAL_ERROR.value,
                },
            },
        }

        definitions["UpdateOperationItem"]["properties"] = {
            field: dict(definitions["Operation"]["properties"][field])
            for field in sorted(self._operation_dispatcher.updatable_fields)
        }
        for field in ("release_date", "due_date", "planned_duration"):
            if field in definitions["UpdateOperationItem"]["properties"]:
                definitions["UpdateOperationItem"]["properties"][field][
                    "x-nullable"
                ] = True
        definitions.update(self._operation_openapi_definitions)
        return definitions
