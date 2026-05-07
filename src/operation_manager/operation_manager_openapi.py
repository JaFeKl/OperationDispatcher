from __future__ import annotations

import asyncio
import threading
import time
from typing import Any
from uuid import UUID

from flask import jsonify, request
from flasgger import swag_from
from pydantic import BaseModel, TypeAdapter, ValidationError

from .models import Operation
from .operation_manager import OperationManager


class OperationManagerOpenAPI:
    def __init__(
        self,
        operation_manager: OperationManager,
        default_operation_name: str = "operation",
    ) -> None:
        self._operation_manager = operation_manager
        resolved_agent_id = operation_manager.schedule.agent_id
        if resolved_agent_id is None:
            raise ValueError("operation_manager.schedule.agent_id must be set")
        self._agent_id = resolved_agent_id
        self._default_operation_name = default_operation_name
        self._payload_model = operation_manager.payload_model
        self._payload_adapter: TypeAdapter[Any] | None = (
            TypeAdapter(self._payload_model)
            if self._payload_model is not None
            else None
        )
        (
            self._payload_openapi_schema,
            self._payload_openapi_definitions,
        ) = self._build_payload_openapi_components()
        self._runtime_thread: threading.Thread | None = None
        self._runtime_last_error: str | None = None

    def get_schedule_response(self) -> tuple[list[dict[str, Any]], int]:
        schedule_payload = [
            operation.model_dump(mode="json")
            for operation in self._operation_manager.get_schedule()
        ]
        return schedule_payload, 200

    def get_schedule_history_response(
        self,
        limit: int | None,
    ) -> tuple[dict[str, Any], int]:
        resolved_limit = 50 if limit is None else limit
        if resolved_limit < 1:
            return {"error": "limit must be greater than 0"}, 400
        if resolved_limit > 1000:
            return {"error": "limit must be less than or equal to 1000"}, 400

        history_operations = self._operation_manager.schedule.history(
            limit=resolved_limit
        )
        payload = {
            "limit": resolved_limit,
            "count": len(history_operations),
            "operations": [
                operation.model_dump(mode="json") for operation in history_operations
            ],
        }
        return payload, 200

    def get_current_operation_response(self) -> tuple[dict[str, Any], int]:
        if self._operation_manager.current_operation is None:
            return {"error": "no current operation"}, 404
        return self._operation_manager.current_operation.model_dump(mode="json"), 200

    def get_next_operation_response(self) -> tuple[dict[str, Any], int]:
        operation = self._operation_manager.schedule.peek()
        if operation is None:
            return {"error": "no next operation"}, 404
        return operation.model_dump(mode="json"), 200

    def add_operation_response(
        self,
        operation_payload: dict[str, Any],
    ) -> tuple[dict[str, Any], int]:
        if "payload" not in operation_payload:
            return {"error": "payload is required"}, 400

        try:
            normalized_operation_payload = dict(operation_payload)
            normalized_operation_payload["payload"] = self._normalize_payload(
                operation_payload["payload"],
            )
        except (ValidationError, TypeError, ValueError) as error:
            return {"error": str(error)}, 400

        normalized_payload = normalized_operation_payload
        normalized_payload["agent_id"] = self._resolved_agent_id()
        normalized_payload.setdefault("name", self._default_operation_name)

        try:
            operation = self._operation_from_payload(normalized_payload)
        except (ValidationError, TypeError, ValueError) as error:
            return {"error": str(error)}, 400

        try:
            self._operation_manager.add(operation)
        except (TypeError, ValueError) as error:
            return {"error": str(error)}, 400

        return operation.model_dump(mode="json"), 201

    def cancel_operation_response(
        self, operation_id: str
    ) -> tuple[dict[str, Any], int]:
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return {"error": str(error)}, 400

        operation = self._operation_manager.cancel(parsed_operation_id)

        if operation is None:
            return {"error": "operation not found"}, 404
        return operation.model_dump(mode="json"), 200

    def get_operation_manager_state_response(self) -> tuple[dict[str, Any], int]:
        operation_manager_state = self._operation_manager.get_state().model_dump(
            mode="json"
        )
        operation_manager_state["runtime_thread_alive"] = (
            self._runtime_thread.is_alive()
            if self._runtime_thread is not None
            else False
        )
        operation_manager_state["runtime_last_error"] = self._runtime_last_error
        return operation_manager_state, 200

    def start_operation_manager_response(self) -> tuple[dict[str, Any], int]:
        if self._operation_manager.is_running:
            state, _ = self.get_operation_manager_state_response()
            return {
                "message": "operation manager is already running",
                "state": state,
            }, 200

        if self._runtime_thread is not None and self._runtime_thread.is_alive():
            state, _ = self.get_operation_manager_state_response()
            return {
                "message": "operation manager runtime thread already active",
                "state": state,
            }, 200

        self._runtime_last_error = None

        def run_operation_manager() -> None:
            try:
                asyncio.run(self._operation_manager.run())
            except Exception as error:
                self._runtime_last_error = str(error)

        self._runtime_thread = threading.Thread(
            target=run_operation_manager,
            name="OperationManagerRuntimeThread",
            daemon=True,
        )
        self._runtime_thread.start()

        deadline = time.time() + 1.0
        while not self._operation_manager.is_running and time.time() < deadline:
            time.sleep(0.01)

        state, _ = self.get_operation_manager_state_response()
        return {
            "message": "operation manager started",
            "state": state,
        }, 200

    def stop_operation_manager_response(self) -> tuple[dict[str, Any], int]:
        if not self._operation_manager.is_running:
            state, _ = self.get_operation_manager_state_response()
            return {
                "message": "operation manager is not running",
                "state": state,
            }, 200

        self._operation_manager.request_stop()
        if self._runtime_thread is not None and self._runtime_thread.is_alive():
            self._runtime_thread.join(timeout=2.0)

        state, _ = self.get_operation_manager_state_response()
        return {
            "message": "operation manager stop requested",
            "state": state,
        }, 200

    def pause_operation_manager_response(self) -> tuple[dict[str, Any], int]:
        if self._operation_manager.is_paused:
            state, _ = self.get_operation_manager_state_response()
            return {
                "message": "operation manager is already paused",
                "state": state,
            }, 200

        self._operation_manager.pause()
        state, _ = self.get_operation_manager_state_response()
        return {
            "message": "operation manager paused",
            "state": state,
        }, 200

    def resume_operation_manager_response(self) -> tuple[dict[str, Any], int]:
        if not self._operation_manager.is_paused:
            state, _ = self.get_operation_manager_state_response()
            return {
                "message": "operation manager is not paused",
                "state": state,
            }, 200

        self._operation_manager.resume()
        state, _ = self.get_operation_manager_state_response()
        return {
            "message": "operation manager resumed",
            "state": state,
        }, 200

    def register_default_endpoints(self, app: Any) -> None:
        self.register_get_schedule_endpoint(app)
        self.register_get_schedule_history_endpoint(app)
        self.register_get_current_operation_endpoint(app)
        self.register_get_next_operation_endpoint(app)
        self.register_add_operation_endpoint(app)
        self.register_cancel_operation_endpoint(app)
        self.register_get_operation_manager_state_endpoint(app)
        self.register_start_operation_manager_endpoint(app)
        self.register_stop_operation_manager_endpoint(app)
        self.register_pause_operation_manager_endpoint(app)
        self.register_resume_operation_manager_endpoint(app)

    def register_get_schedule_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/schedule",
        endpoint_name: str = "get_schedule",
    ) -> None:
        openapi_spec = self.get_schedule_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_schedule_endpoint() -> tuple[Any, int]:
            payload, status_code = self.get_schedule_response()
            return jsonify(payload), status_code

    def register_get_schedule_history_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/history",
        endpoint_name: str = "get_schedule_history",
    ) -> None:
        openapi_spec = self.get_schedule_history_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_schedule_history_endpoint() -> tuple[Any, int]:
            limit_value = request.args.get("limit")
            if limit_value is None:
                parsed_limit = None
            else:
                try:
                    parsed_limit = int(limit_value)
                except ValueError:
                    return jsonify({"error": "limit must be an integer"}), 400

            payload, status_code = self.get_schedule_history_response(parsed_limit)
            return jsonify(payload), status_code

    def register_get_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/current_operation",
        endpoint_name: str = "get_current_operation",
    ) -> None:
        openapi_spec = self.get_current_operation_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_current_operation_endpoint() -> tuple[Any, int]:
            payload, status_code = self.get_current_operation_response()
            return jsonify(payload), status_code

    def register_get_next_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/next_operation",
        endpoint_name: str = "get_next_operation",
    ) -> None:
        openapi_spec = self.get_next_operation_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_next_operation_endpoint() -> tuple[Any, int]:
            payload, status_code = self.get_next_operation_response()
            return jsonify(payload), status_code

    def register_add_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/add_operation",
        endpoint_name: str = "add_operation",
    ) -> None:
        openapi_spec = self.add_operation_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def add_operation_endpoint() -> tuple[Any, int]:
            payload = request.get_json(silent=True) or {}
            response_body, status_code = self.add_operation_response(payload)
            return jsonify(response_body), status_code

    def register_cancel_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/cancel_operation",
        endpoint_name: str = "cancel_operation",
    ) -> None:
        openapi_spec = self.cancel_operation_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def cancel_operation_endpoint() -> tuple[Any, int]:
            payload = request.get_json(silent=True) or {}
            operation_id = payload.get("operation_id", "")
            response_body, status_code = self.cancel_operation_response(operation_id)
            return jsonify(response_body), status_code

    def register_get_operation_manager_state_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/state",
        endpoint_name: str = "get_operation_manager_state",
    ) -> None:
        openapi_spec = self.get_operation_manager_state_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_operation_manager_state_endpoint() -> tuple[Any, int]:
            payload, status_code = self.get_operation_manager_state_response()
            return jsonify(payload), status_code

    def register_start_operation_manager_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/start",
        endpoint_name: str = "start",
    ) -> None:
        openapi_spec = self.start_operation_manager_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def start_operation_manager_endpoint() -> tuple[Any, int]:
            payload, status_code = self.start_operation_manager_response()
            return jsonify(payload), status_code

    def register_stop_operation_manager_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/stop",
        endpoint_name: str = "stop",
    ) -> None:
        openapi_spec = self.stop_operation_manager_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def stop_operation_manager_endpoint() -> tuple[Any, int]:
            payload, status_code = self.stop_operation_manager_response()
            return jsonify(payload), status_code

    def register_pause_operation_manager_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/pause",
        endpoint_name: str = "pause",
    ) -> None:
        openapi_spec = self.pause_operation_manager_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def pause_operation_manager_endpoint() -> tuple[Any, int]:
            payload, status_code = self.pause_operation_manager_response()
            return jsonify(payload), status_code

    def register_resume_operation_manager_endpoint(
        self,
        app: Any,
        route: str = "/operation_manager/resume",
        endpoint_name: str = "resume",
    ) -> None:
        openapi_spec = self.resume_operation_manager_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def resume_operation_manager_endpoint() -> tuple[Any, int]:
            payload, status_code = self.resume_operation_manager_response()
            return jsonify(payload), status_code

    @staticmethod
    def get_schedule_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Queued operations ordered by operation manager priority rules.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/ScheduledOperation"},
                    },
                }
            },
        }

    @staticmethod
    def get_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current running operation.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No operation is currently running.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_schedule_history_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
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
                    "description": "Maximum number of historic operations to return.",
                }
            ],
            "responses": {
                200: {
                    "description": "Most recent historic operations.",
                    "schema": {"$ref": "#/definitions/OperationManagerHistoryResponse"},
                },
                400: {
                    "description": "Invalid limit value.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_next_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Next queued operation without dequeuing it.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No next operation in queue.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def add_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "in": "body",
                    "name": "body",
                    "required": True,
                    "schema": {"$ref": "#/definitions/AddOperationRequest"},
                }
            ],
            "responses": {
                201: {
                    "description": "Operation successfully added to schedule.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                400: {
                    "description": "Invalid operation payload.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def cancel_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager"],
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "parameters": [
                {
                    "in": "body",
                    "name": "body",
                    "required": True,
                    "schema": {"$ref": "#/definitions/CancelOperationRequest"},
                }
            ],
            "responses": {
                200: {
                    "description": "Operation cancelled (or stopped if currently running).",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                400: {
                    "description": "Invalid operation id.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
                404: {
                    "description": "Operation id not found.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_operation_manager_state_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current operation manager runtime state.",
                    "schema": {"$ref": "#/definitions/OperationManagerState"},
                }
            },
        }

    @staticmethod
    def start_operation_manager_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Operation manager start request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationManagerRuntimeActionResponse"
                    },
                }
            },
        }

    @staticmethod
    def stop_operation_manager_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Operation manager stop request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationManagerRuntimeActionResponse"
                    },
                }
            },
        }

    @staticmethod
    def pause_operation_manager_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Operation manager pause request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationManagerRuntimeActionResponse"
                    },
                }
            },
        }

    @staticmethod
    def resume_operation_manager_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Manager Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Operation manager resume request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationManagerRuntimeActionResponse"
                    },
                }
            },
        }

    def get_openapi_definitions(self) -> dict[str, Any]:
        definitions: dict[str, Any] = {
            "ErrorResponse": {
                "type": "object",
                "properties": {
                    "error": {"type": "string", "example": "operation not found"},
                },
                "required": ["error"],
            },
            "CancelOperationRequest": {
                "type": "object",
                "properties": {
                    "operation_id": {
                        "type": "string",
                        "format": "uuid",
                        "example": "123e4567-e89b-12d3-a456-426614174000",
                    },
                },
                "required": ["operation_id"],
            },
            "AddOperationRequest": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "example": "collect_metrics"},
                    "payload": self._payload_openapi_schema,
                    "priority": {"type": "integer", "example": 10},
                    "time_window": {
                        "$ref": "#/definitions/TimeWindow",
                    },
                },
                "required": ["payload"],
            },
            "TimeWindow": {
                "type": "object",
                "properties": {
                    "start": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T10:00:00Z",
                    },
                    "end": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T10:30:00Z",
                    },
                },
                "required": ["start", "end"],
            },
            "ScheduledOperation": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "format": "uuid",
                        "example": "123e4567-e89b-12d3-a456-426614174000",
                    },
                    "name": {"type": "string", "example": "collect_metrics"},
                    "agent_id": {"type": "string", "example": "agent-1"},
                    "payload": self._payload_openapi_schema,
                    "priority": {"type": "integer", "example": 10},
                    "lifecycle_status": {
                        "type": "string",
                        "enum": ["queued", "ready", "running", "finished"],
                        "example": "queued",
                    },
                    "execution_outcome": {
                        "type": "string",
                        "enum": ["none", "succeeded", "failed"],
                        "example": "none",
                    },
                    "termination_reason": {
                        "type": "string",
                        "enum": [
                            "none",
                            "cancelled_before_start",
                            "cancelled_during_run",
                            "stopped",
                            "timeout",
                            "dependency_failed",
                        ],
                        "example": "none",
                    },
                    "created_at": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T10:00:00Z",
                    },
                    "start_time": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T10:01:00Z",
                        "description": "Null until operation starts.",
                    },
                    "finish_time": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T10:31:00Z",
                        "description": "Null until operation finishes.",
                    },
                    "time_window": {
                        "$ref": "#/definitions/TimeWindow",
                    },
                },
                "required": [
                    "id",
                    "name",
                    "agent_id",
                    "payload",
                    "priority",
                    "lifecycle_status",
                    "execution_outcome",
                    "termination_reason",
                    "created_at",
                    "start_time",
                    "finish_time",
                    "time_window",
                ],
            },
            "OperationManagerState": {
                "type": "object",
                "properties": {
                    "is_running": {"type": "boolean", "example": True},
                    "is_paused": {"type": "boolean", "example": False},
                    "queue_size": {"type": "integer", "example": 3},
                    "current_operation": {
                        "oneOf": [
                            {"$ref": "#/definitions/ScheduledOperation"},
                            {"type": "null"},
                        ]
                    },
                    "running_since": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2026-05-06T12:00:00+00:00",
                        "description": "Null when operation manager is not running.",
                    },
                    "uptime_seconds": {
                        "type": "number",
                        "example": 12.5,
                        "description": "Null when operation manager is not running.",
                    },
                    "runtime_thread_alive": {"type": "boolean", "example": True},
                    "runtime_last_error": {
                        "type": "string",
                        "example": "",
                        "description": "Last runtime error string, empty when none.",
                    },
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
            "OperationManagerRuntimeActionResponse": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "example": "operation manager started",
                    },
                    "state": {"$ref": "#/definitions/OperationManagerState"},
                },
                "required": ["message", "state"],
            },
            "OperationManagerHistoryResponse": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "example": 50},
                    "count": {"type": "integer", "example": 2},
                    "operations": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/ScheduledOperation"},
                    },
                },
                "required": ["limit", "count", "operations"],
            },
        }
        definitions.update(self._payload_openapi_definitions)
        return definitions

    @staticmethod
    def _operation_from_payload(operation_payload: dict[str, Any]) -> Operation:
        return Operation.model_validate(operation_payload)

    def _resolved_agent_id(self) -> str:
        return self._agent_id

    def _normalize_payload(self, payload: Any) -> dict[str, Any]:
        if self._payload_adapter is None:
            if not isinstance(payload, dict):
                raise TypeError("payload must be an object")
            return payload

        validated_payload = self._payload_adapter.validate_python(payload)
        if isinstance(validated_payload, BaseModel):
            return validated_payload.model_dump()
        if isinstance(validated_payload, dict):
            return validated_payload
        raise TypeError("payload model must resolve to an object")

    def _build_payload_openapi_components(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if self._payload_adapter is None:
            return {
                "type": "object",
                "additionalProperties": True,
                "example": {"task": "collect_metrics"},
            }, {}

        payload_schema = self._payload_adapter.json_schema()
        payload_definitions: dict[str, Any] = {
            definition_name: self._convert_schema_to_openapi(definition_schema)
            for definition_name, definition_schema in payload_schema.get(
                "$defs", {}
            ).items()
        }

        root_schema = self._convert_schema_to_openapi(
            {key: value for key, value in payload_schema.items() if key != "$defs"}
        )

        if "$ref" in root_schema:
            return {"$ref": root_schema["$ref"]}, payload_definitions

        definition_name = payload_schema.get("title") or "OperationPayload"
        root_schema.pop("title", None)
        payload_definitions[definition_name] = root_schema
        return {"$ref": f"#/definitions/{definition_name}"}, payload_definitions

    @classmethod
    def _convert_schema_to_openapi(cls, schema: Any) -> Any:
        if isinstance(schema, dict):
            converted_schema: dict[str, Any] = {}
            for key, value in schema.items():
                if key == "$defs":
                    continue
                if key == "$ref" and isinstance(value, str):
                    converted_schema[key] = value.replace(
                        "#/$defs/",
                        "#/definitions/",
                    )
                else:
                    converted_schema[key] = cls._convert_schema_to_openapi(value)
            return converted_schema

        if isinstance(schema, list):
            return [cls._convert_schema_to_openapi(item) for item in schema]

        return schema
