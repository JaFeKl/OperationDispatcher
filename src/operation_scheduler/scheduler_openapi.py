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
from .scheduler import Scheduler


class SchedulerOpenAPI:
    def __init__(
        self,
        scheduler: Scheduler,
        agent_id: str | None = None,
        default_operation_name: str = "operation",
        payload_model: Any | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._agent_id = (
            agent_id if agent_id is not None else scheduler.schedule.agent_id
        )
        self._default_operation_name = default_operation_name
        self._payload_model = (
            payload_model if payload_model is not None else scheduler.payload_model
        )
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
            for operation in self._scheduler.get_schedule()
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

        history_operations = self._scheduler.schedule.history(limit=resolved_limit)
        payload = {
            "limit": resolved_limit,
            "count": len(history_operations),
            "operations": [
                operation.model_dump(mode="json") for operation in history_operations
            ],
        }
        return payload, 200

    def get_current_operation_response(self) -> tuple[dict[str, Any], int]:
        if self._scheduler.current_operation is None:
            return {"error": "no current operation"}, 404
        return self._scheduler.current_operation.model_dump(mode="json"), 200

    def get_next_operation_response(self) -> tuple[dict[str, Any], int]:
        operation = self._scheduler.schedule.peek()
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

        resolved_agent_id = self._resolved_agent_id()
        if resolved_agent_id is None:
            return {"error": "agent_id is not configured for SchedulerOpenAPI"}, 400

        normalized_payload = normalized_operation_payload
        normalized_payload["agent_id"] = resolved_agent_id
        normalized_payload.setdefault("name", self._default_operation_name)

        try:
            operation = self._operation_from_payload(normalized_payload)
        except (ValidationError, TypeError, ValueError) as error:
            return {"error": str(error)}, 400

        try:
            self._scheduler.add(operation)
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

        operation = self._scheduler.cancel(parsed_operation_id)
        if operation is None:
            return {"error": "operation not found"}, 404
        return operation.model_dump(mode="json"), 200

    def get_scheduler_state_response(self) -> tuple[dict[str, Any], int]:
        scheduler_state = self._scheduler.get_state()
        scheduler_state["runtime_thread_alive"] = (
            self._runtime_thread.is_alive()
            if self._runtime_thread is not None
            else False
        )
        scheduler_state["runtime_last_error"] = self._runtime_last_error
        return scheduler_state, 200

    def start_scheduler_response(self) -> tuple[dict[str, Any], int]:
        if self._scheduler.is_running:
            state, _ = self.get_scheduler_state_response()
            return {
                "message": "scheduler is already running",
                "state": state,
            }, 200

        if self._runtime_thread is not None and self._runtime_thread.is_alive():
            state, _ = self.get_scheduler_state_response()
            return {
                "message": "scheduler runtime thread already active",
                "state": state,
            }, 200

        self._runtime_last_error = None

        def run_scheduler() -> None:
            try:
                asyncio.run(self._scheduler.run())
            except Exception as error:
                self._runtime_last_error = str(error)

        self._runtime_thread = threading.Thread(
            target=run_scheduler,
            name="SchedulerRuntimeThread",
            daemon=True,
        )
        self._runtime_thread.start()

        deadline = time.time() + 1.0
        while not self._scheduler.is_running and time.time() < deadline:
            time.sleep(0.01)

        state, _ = self.get_scheduler_state_response()
        return {
            "message": "scheduler started",
            "state": state,
        }, 200

    def stop_scheduler_response(self) -> tuple[dict[str, Any], int]:
        if not self._scheduler.is_running:
            state, _ = self.get_scheduler_state_response()
            return {
                "message": "scheduler is not running",
                "state": state,
            }, 200

        self._scheduler.request_stop()
        if self._runtime_thread is not None and self._runtime_thread.is_alive():
            self._runtime_thread.join(timeout=2.0)

        state, _ = self.get_scheduler_state_response()
        return {
            "message": "scheduler stop requested",
            "state": state,
        }, 200

    def register_default_endpoints(self, app: Any) -> None:
        self.register_get_schedule_endpoint(app)
        self.register_get_schedule_history_endpoint(app)
        self.register_get_current_operation_endpoint(app)
        self.register_get_next_operation_endpoint(app)
        self.register_add_operation_endpoint(app)
        self.register_cancel_operation_endpoint(app)
        self.register_get_scheduler_state_endpoint(app)
        self.register_start_scheduler_endpoint(app)
        self.register_stop_scheduler_endpoint(app)

    def register_get_schedule_endpoint(
        self,
        app: Any,
        route: str = "/scheduler/schedule",
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
        route: str = "/scheduler/history",
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
        route: str = "/scheduler/current_operation",
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
        route: str = "/scheduler/next_operation",
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
        route: str = "/scheduler/add_operation",
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
        route: str = "/scheduler/cancel_operation",
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

    def register_get_scheduler_state_endpoint(
        self,
        app: Any,
        route: str = "/scheduler/state",
        endpoint_name: str = "get_scheduler_state",
    ) -> None:
        openapi_spec = self.get_scheduler_state_openapi_spec()

        @app.get(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def get_scheduler_state_endpoint() -> tuple[Any, int]:
            payload, status_code = self.get_scheduler_state_response()
            return jsonify(payload), status_code

    def register_start_scheduler_endpoint(
        self,
        app: Any,
        route: str = "/scheduler/start",
        endpoint_name: str = "start",
    ) -> None:
        openapi_spec = self.start_scheduler_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def start_scheduler_endpoint() -> tuple[Any, int]:
            payload, status_code = self.start_scheduler_response()
            return jsonify(payload), status_code

    def register_stop_scheduler_endpoint(
        self,
        app: Any,
        route: str = "/scheduler/stop",
        endpoint_name: str = "stop",
    ) -> None:
        openapi_spec = self.stop_scheduler_openapi_spec()

        @app.post(route, endpoint=endpoint_name)
        @swag_from(openapi_spec)
        def stop_scheduler_endpoint() -> tuple[Any, int]:
            payload, status_code = self.stop_scheduler_response()
            return jsonify(payload), status_code

    @staticmethod
    def get_schedule_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Scheduler"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Queued operations ordered by scheduler priority rules.",
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
            "tags": ["Scheduler"],
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
            "tags": ["Scheduler"],
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
                    "schema": {"$ref": "#/definitions/SchedulerHistoryResponse"},
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
            "tags": ["Scheduler"],
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
            "tags": ["Scheduler"],
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
            "tags": ["Scheduler"],
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
    def get_scheduler_state_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Scheduler Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current scheduler runtime state.",
                    "schema": {"$ref": "#/definitions/SchedulerState"},
                }
            },
        }

    @staticmethod
    def start_scheduler_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Scheduler Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Scheduler start request result.",
                    "schema": {"$ref": "#/definitions/SchedulerRuntimeActionResponse"},
                }
            },
        }

    @staticmethod
    def stop_scheduler_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Scheduler Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Scheduler stop request result.",
                    "schema": {"$ref": "#/definitions/SchedulerRuntimeActionResponse"},
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
                    "runtime_status": {
                        "type": "string",
                        "enum": ["pending", "running", "finished"],
                        "example": "pending",
                    },
                    "result_status": {
                        "type": "string",
                        "enum": ["none", "succeeded", "failed", "cancelled", "stopped"],
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
                    "runtime_status",
                    "result_status",
                    "created_at",
                    "start_time",
                    "finish_time",
                    "time_window",
                ],
            },
            "SchedulerState": {
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
                        "description": "Null when scheduler is not running.",
                    },
                    "uptime_seconds": {
                        "type": "number",
                        "example": 12.5,
                        "description": "Null when scheduler is not running.",
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
            "SchedulerRuntimeActionResponse": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "example": "scheduler started",
                    },
                    "state": {"$ref": "#/definitions/SchedulerState"},
                },
                "required": ["message", "state"],
            },
            "SchedulerHistoryResponse": {
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

    def _resolved_agent_id(self) -> str | None:
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
