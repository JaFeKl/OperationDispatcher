from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any
from uuid import UUID

from flask import jsonify, request
from flasgger import swag_from
from pydantic import BaseModel, TypeAdapter, ValidationError

from .models import (
    EventType,
    ExecutionState,
    Operation,
    OperationHistoryEntry,
    ScheduledOperation,
)
from .operation_dispatcher import OperationDispatcher


class OperationDispatcherOpenAPI:
    def __init__(
        self,
        operation_dispatcher: OperationDispatcher,
        default_operation_name: str = "operation",
    ) -> None:
        self._operation_dispatcher = operation_dispatcher
        self._resource_id = operation_dispatcher.dispatch_queue.resource_id
        self._default_operation_name = default_operation_name

        self._operation_model = operation_dispatcher.operation_model
        self._operation_adapter: TypeAdapter[Any] = TypeAdapter(self._operation_model)

        (
            self._operation_openapi_schema,
            self._operation_openapi_definitions,
        ) = self._build_operation_openapi_components()

        self._runtime_thread: threading.Thread | None = None
        self._runtime_last_error: str | None = None
        self._runtime_lock = threading.Lock()
        self._runtime_startup_timeout_seconds = 1.0
        self._runtime_stop_join_timeout_seconds = 2.0

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

    def get_dispatch_queue_response(self) -> tuple[list[dict[str, Any]], int]:
        queue_payload = [
            scheduled_operation.model_dump(mode="json")
            for scheduled_operation in self._operation_dispatcher.get_schedule()
        ]
        return queue_payload, 200

    def get_dispatch_history_response(
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

        history_entries = self._operation_dispatcher.get_history_entries(
            limit=resolved_limit
        )
        payload = {
            "limit": resolved_limit,
            "count": len(history_entries),
            "entries": [entry.model_dump(mode="json") for entry in history_entries],
        }
        return payload, 200

    def get_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_scheduled_operation = (
            self._operation_dispatcher.current_scheduled_operation
        )
        if current_scheduled_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )
        return current_scheduled_operation.model_dump(mode="json"), 200

    def get_next_operation_response(self) -> tuple[dict[str, Any], int]:
        scheduled_operation = self._operation_dispatcher.dispatch_queue.peek()
        if scheduled_operation is None:
            return self._error_response(
                message="no next operation",
                code="no_next_operation",
                status_code=404,
            )
        return scheduled_operation.model_dump(mode="json"), 200

    def add_operation_response(
        self,
        request_payload: Any,
    ) -> tuple[dict[str, Any] | list[dict[str, Any]], int]:
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

        scheduled_operations: list[ScheduledOperation] = []
        for index, operation_request in enumerate(request_payload):
            if not isinstance(operation_request, dict):
                return self._error_response(
                    message="each list item must be a scheduled operation object",
                    code="invalid_operations",
                    status_code=400,
                    details={"index": index},
                )

            scheduled_operation_result, scheduled_operation_status = (
                self._build_scheduled_operation(operation_request)
            )
            if scheduled_operation_status != 201:
                scheduled_operation_result.setdefault("details", {})["index"] = index
                return scheduled_operation_result, scheduled_operation_status

            scheduled_operations.append(scheduled_operation_result)

        for scheduled_operation in scheduled_operations:
            try:
                self._operation_dispatcher.add(scheduled_operation)
            except (TypeError, ValueError) as error:
                return self._error_response(
                    message=str(error),
                    code="operation_rejected",
                    status_code=400,
                )

        return [
            scheduled_operation.model_dump(mode="json")
            for scheduled_operation in scheduled_operations
        ], 201

    def _build_scheduled_operation(
        self,
        request_payload: dict[str, Any],
    ) -> tuple[ScheduledOperation | dict[str, Any], int]:
        operation_payload = request_payload.get("operation")
        if operation_payload is None:
            return self._error_response(
                message="operation is required",
                code="missing_operation",
                status_code=400,
            )

        try:
            operation = self._normalize_operation(operation_payload)
        except (ValidationError, TypeError, ValueError) as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation",
                status_code=400,
            )

        try:
            scheduled_operation = ScheduledOperation.model_validate(
                {
                    "operation": operation,
                    "resource_id": request_payload.get(
                        "resource_id",
                        self._resource_id,
                    ),
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
        self, operation_id: str
    ) -> tuple[dict[str, Any], int]:
        try:
            parsed_operation_id = UUID(operation_id)
        except ValueError as error:
            return self._error_response(
                message=str(error),
                code="invalid_operation_id",
                status_code=400,
            )

        scheduled_operation = self._operation_dispatcher.cancel(parsed_operation_id)

        if scheduled_operation is None:
            return self._error_response(
                message="operation not found",
                code="operation_not_found",
                status_code=404,
            )
        return scheduled_operation.model_dump(mode="json"), 200

    def get_operation_dispatcher_state_response(self) -> tuple[dict[str, Any], int]:
        dispatcher_state = self._operation_dispatcher.get_state().model_dump(
            mode="json"
        )
        dispatcher_state["runtime_thread_alive"] = (
            self._runtime_thread.is_alive()
            if self._runtime_thread is not None
            else False
        )
        dispatcher_state["runtime_last_error"] = self._runtime_last_error
        return dispatcher_state, 200

    def start_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        with self._runtime_lock:
            if self._operation_dispatcher.is_running:
                state, _ = self.get_operation_dispatcher_state_response()
                return {
                    "message": "operation dispatcher is already running",
                    "state": state,
                }, 409

            if self._runtime_thread is not None and self._runtime_thread.is_alive():
                state, _ = self.get_operation_dispatcher_state_response()
                return {
                    "message": "operation dispatcher runtime thread already active",
                    "state": state,
                }, 409

            self._runtime_last_error = None

            def run_operation_dispatcher() -> None:
                try:
                    asyncio.run(self._operation_dispatcher.run())
                except Exception as error:
                    self._runtime_last_error = str(error)

            self._runtime_thread = threading.Thread(
                target=run_operation_dispatcher,
                name="OperationDispatcherRuntimeThread",
                daemon=True,
            )
            self._runtime_thread.start()

        deadline = time.time() + self._runtime_startup_timeout_seconds
        while not self._operation_dispatcher.is_running and time.time() < deadline:
            time.sleep(0.01)

        state, _ = self.get_operation_dispatcher_state_response()
        if self._operation_dispatcher.is_running:
            return {
                "message": "operation dispatcher started",
                "state": state,
            }, 202

        if self._runtime_last_error:
            return {
                "message": "operation dispatcher failed to start",
                "state": state,
                "error": self._runtime_last_error,
            }, 500

        return {
            "message": "operation dispatcher start requested",
            "state": state,
        }, 202

    def stop_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        with self._runtime_lock:
            runtime_active = (
                self._runtime_thread is not None and self._runtime_thread.is_alive()
            )
            if not self._operation_dispatcher.is_running and not runtime_active:
                state, _ = self.get_operation_dispatcher_state_response()
                return {
                    "message": "operation dispatcher is not running",
                    "state": state,
                }, 409

            self._operation_dispatcher.request_stop()
            runtime_thread = self._runtime_thread

        if runtime_thread is not None and runtime_thread.is_alive():
            runtime_thread.join(timeout=self._runtime_stop_join_timeout_seconds)

        state, _ = self.get_operation_dispatcher_state_response()
        return {
            "message": "operation dispatcher stopped",
            "state": state,
        }, 202

    def resume_operation_dispatcher_response(self) -> tuple[dict[str, Any], int]:
        if not self._operation_dispatcher.is_paused:
            state, _ = self.get_operation_dispatcher_state_response()
            return {
                "message": "operation dispatcher is not paused",
                "state": state,
            }, 409

        self._operation_dispatcher.resume()
        state, _ = self.get_operation_dispatcher_state_response()
        return {
            "message": "operation dispatcher resumed",
            "state": state,
        }, 200

    def cancel_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_operation = self._operation_dispatcher.current_scheduled_operation
        if current_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )

        cancelled_operation = self._operation_dispatcher.cancel(
            current_operation.operation.id
        )
        if cancelled_operation is None:
            return self._error_response(
                message="current operation cancellation denied",
                code="current_operation_cancellation_denied",
                status_code=409,
            )

        return cancelled_operation.model_dump(mode="json"), 200

    def pause_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_operation = self._operation_dispatcher.current_scheduled_operation
        if current_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )

        current_execution = self._operation_dispatcher.current_execution
        if (
            current_execution is None
            or current_execution.state is not ExecutionState.RUNNING
        ):
            return self._error_response(
                message="current operation is not running",
                code="current_operation_not_running",
                status_code=409,
            )

        try:
            is_paused = self._operation_dispatcher.pause_current()
        except RuntimeError as error:
            if str(error) == "current operation is not running":
                return self._error_response(
                    message="current operation is not running",
                    code="current_operation_not_running",
                    status_code=409,
                )
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )

        if not is_paused:
            return self._error_response(
                message="current operation pause denied",
                code="current_operation_pause_denied",
                status_code=409,
            )

        paused_operation = self._operation_dispatcher.current_scheduled_operation
        if paused_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )
        return paused_operation.model_dump(mode="json"), 200

    def resume_current_operation_response(self) -> tuple[dict[str, Any], int]:
        current_operation = self._operation_dispatcher.current_scheduled_operation
        if current_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )

        current_execution = self._operation_dispatcher.current_execution
        if (
            current_execution is None
            or current_execution.state is not ExecutionState.PAUSED
        ):
            return self._error_response(
                message="current operation is not paused",
                code="current_operation_not_paused",
                status_code=409,
            )

        self._operation_dispatcher.resume()
        if self._operation_dispatcher.is_paused:
            return self._error_response(
                message="current operation resume denied",
                code="current_operation_resume_denied",
                status_code=409,
            )

        resumed_operation = self._operation_dispatcher.current_scheduled_operation
        if resumed_operation is None:
            return self._error_response(
                message="no current operation",
                code="no_current_operation",
                status_code=404,
            )

        return resumed_operation.model_dump(mode="json"), 200

    def register_default_endpoints(self, app: Any) -> None:
        self.register_get_dispatch_queue_endpoint(app)
        self.register_get_dispatch_history_endpoint(app)
        self.register_get_current_operation_endpoint(app)
        self.register_get_next_operation_endpoint(app)
        self.register_add_operation_endpoint(app)
        self.register_cancel_operation_endpoint(app)
        self.register_get_operation_dispatcher_state_endpoint(app)
        self.register_start_operation_dispatcher_endpoint(app)
        self.register_stop_operation_dispatcher_endpoint(app)
        self.register_resume_operation_dispatcher_endpoint(app)
        self.register_cancel_current_operation_endpoint(app)
        self.register_pause_current_operation_endpoint(app)
        self.register_resume_current_operation_endpoint(app)

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
        def endpoint() -> tuple[Any, int]:
            payload, status_code = response_handler()
            return jsonify(payload), status_code

    @staticmethod
    def _parse_json_body() -> Any:
        payload = request.get_json(silent=True)
        return payload

    def register_get_dispatch_queue_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/queue",
        endpoint_name: str = "get_dispatch_queue",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_dispatch_queue_openapi_spec(),
            response_handler=self.get_dispatch_queue_response,
        )

    def register_get_dispatch_history_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/history",
        endpoint_name: str = "get_dispatch_history",
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

            return self.get_dispatch_history_response(parsed_limit)

        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_dispatch_history_openapi_spec(),
            response_handler=response_handler,
        )

    def register_get_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/current_operation",
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

    def register_get_next_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/next",
        endpoint_name: str = "get_next_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="GET",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.get_next_operation_openapi_spec(),
            response_handler=self.get_next_operation_response,
        )

    def register_add_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/add",
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
        route: str = "/operation_dispatcher/cancel",
        endpoint_name: str = "cancel_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.cancel_operation_openapi_spec(),
            response_handler=lambda: self.cancel_operation_response(
                self._parse_json_body().get("operation_id", "")
            ),
        )

    def register_get_operation_dispatcher_state_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/state",
        endpoint_name: str = "get_operation_dispatcher_state",
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
        route: str = "/operation_dispatcher/start",
        endpoint_name: str = "start",
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
        route: str = "/operation_dispatcher/stop",
        endpoint_name: str = "stop",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.stop_operation_dispatcher_openapi_spec(),
            response_handler=self.stop_operation_dispatcher_response,
        )

    def register_resume_operation_dispatcher_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/resume",
        endpoint_name: str = "resume",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.resume_operation_dispatcher_openapi_spec(),
            response_handler=self.resume_operation_dispatcher_response,
        )

    def register_cancel_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/current_operation/cancel",
        endpoint_name: str = "cancel_current_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.cancel_current_operation_openapi_spec(),
            response_handler=self.cancel_current_operation_response,
        )

    def register_pause_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/current_operation/pause",
        endpoint_name: str = "pause_current_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.pause_current_operation_openapi_spec(),
            response_handler=self.pause_current_operation_response,
        )

    def register_resume_current_operation_endpoint(
        self,
        app: Any,
        route: str = "/operation_dispatcher/current_operation/resume",
        endpoint_name: str = "resume_current_operation",
    ) -> None:
        self._register_json_endpoint(
            app,
            method="POST",
            route=route,
            endpoint_name=endpoint_name,
            openapi_spec=self.resume_current_operation_openapi_spec(),
            response_handler=self.resume_current_operation_response,
        )

    @staticmethod
    def get_dispatch_queue_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Queued scheduled operations.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/ScheduledOperation"},
                    },
                }
            },
        }

    @staticmethod
    def get_dispatch_history_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
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
                    "description": "Most recent completed operations.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherHistoryResponse"
                    },
                },
                400: {
                    "description": "Invalid limit value.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current running operation.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No current operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def get_next_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Next queued operation.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No next operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def add_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
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
                    "description": "Operations added to dispatch queue.",
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/ScheduledOperation"},
                    },
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
            "tags": ["Operation Dispatcher"],
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
                    "description": "Operation cancelled.",
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
    def get_operation_dispatcher_state_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher Runtime"],
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
            "tags": ["Operation Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                202: {
                    "description": "Operation dispatcher start request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "description": "Operation dispatcher is already running.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def stop_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                202: {
                    "description": "Operation dispatcher stop request result.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "description": "Operation dispatcher is not running.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def resume_operation_dispatcher_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher Runtime"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Operation dispatcher resumed.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
                409: {
                    "description": "Operation dispatcher is not paused.",
                    "schema": {
                        "$ref": "#/definitions/OperationDispatcherRuntimeActionResponse"
                    },
                },
            },
        }

    @staticmethod
    def cancel_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current operation cancelled.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No current operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
                409: {
                    "description": "Current operation cancel request denied.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def pause_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current operation paused.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No current operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
                409: {
                    "description": "Current operation is not running or pause request denied.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    @staticmethod
    def resume_current_operation_openapi_spec() -> dict[str, Any]:
        return {
            "tags": ["Operation Dispatcher"],
            "produces": ["application/json"],
            "responses": {
                200: {
                    "description": "Current operation resumed.",
                    "schema": {"$ref": "#/definitions/ScheduledOperation"},
                },
                404: {
                    "description": "No current operation.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
                409: {
                    "description": "Current operation resume request denied.",
                    "schema": {"$ref": "#/definitions/ErrorResponse"},
                },
            },
        }

    def get_openapi_definitions(self) -> dict[str, Any]:
        definitions: dict[str, Any] = {
            "ErrorResponse": {
                "type": "object",
                "properties": {
                    "error": {"type": "string", "example": "operation not found"},
                    "message": {"type": "string", "example": "operation not found"},
                    "code": {"type": "string", "example": "operation_not_found"},
                    "details": {
                        "type": "object",
                        "additionalProperties": True,
                    },
                },
                "required": ["error", "message", "code"],
            },
            "ScheduledOperation": {
                "type": "object",
                "properties": {
                    "operation": self._operation_openapi_schema,
                    "resource_id": {"type": "string", "example": "resource-1"},
                    "priority": {"type": "integer", "example": 10},
                    "release_date": {
                        "type": "string",
                        "format": "date-time",
                    },
                    "planned_duration": {
                        "type": "string",
                    },
                    "due_date": {
                        "type": "string",
                        "format": "date-time",
                    },
                    "created_at": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
                "required": [
                    "operation",
                    "resource_id",
                    "priority",
                    "created_at",
                ],
            },
            "OperationExecution": {
                "type": "object",
                "properties": {
                    "operation_id": {"type": "string", "format": "uuid"},
                    "state": {"type": "string"},
                    "outcome": {"type": "string"},
                    "termination_reason": {"type": "string"},
                    "retry_count": {"type": "integer"},
                    "start_time": {"type": "string", "format": "date-time"},
                    "finish_time": {"type": "string", "format": "date-time"},
                },
                "required": [
                    "operation_id",
                    "state",
                    "outcome",
                    "termination_reason",
                    "retry_count",
                ],
            },
            "DispatchEvent": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "event_type": {
                        "type": "string",
                        "enum": [event.value for event in EventType],
                    },
                    "created_at": {
                        "type": "string",
                        "format": "date-time",
                    },
                    "resource_id": {
                        "type": "string",
                    },
                    "operation_id": {
                        "type": "string",
                        "format": "uuid",
                    },
                    "data": {"$ref": "#/definitions/EventData"},
                },
                "required": ["id", "event_type", "created_at", "data"],
            },
            "OperationHistoryEntry": {
                "type": "object",
                "properties": {
                    "scheduled_operation": {"$ref": "#/definitions/ScheduledOperation"},
                    "execution": {"$ref": "#/definitions/OperationExecution"},
                    "events": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/DispatchEvent"},
                    },
                },
                "required": ["scheduled_operation", "execution", "events"],
            },
            "EventData": {
                "type": "object",
                "properties": {
                    "request_decision": {"$ref": "#/definitions/RequestDecision"},
                },
                "additionalProperties": True,
            },
            "RequestDecision": {
                "type": "object",
                "properties": {
                    "is_allowed": {"type": "boolean"},
                    "reason": {"type": "string"},
                    "metadata": {
                        "type": "object",
                        "additionalProperties": True,
                    },
                },
                "required": ["is_allowed", "metadata"],
            },
            "OperationDispatcherState": {
                "type": "object",
                "properties": {
                    "is_running": {"type": "boolean"},
                    "is_paused": {"type": "boolean"},
                    "queue_size": {"type": "integer"},
                    "current_operation": {
                        "oneOf": [
                            self._operation_openapi_schema,
                            {"type": "null"},
                        ]
                    },
                    "running_since": {
                        "type": "string",
                        "format": "date-time",
                    },
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
            "OperationDispatcherHistoryResponse": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer"},
                    "count": {"type": "integer"},
                    "entries": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/OperationHistoryEntry"},
                    },
                },
                "required": ["limit", "count", "entries"],
            },
            "AddOperationRequest": {
                "type": "array",
                "items": {"$ref": "#/definitions/AddOperationItem"},
                "description": "List of scheduled operations to add.",
            },
            "AddOperationItem": {
                "type": "object",
                "properties": {
                    "operation": self._operation_openapi_schema,
                    "resource_id": {"type": "string", "example": "resource-1"},
                    "priority": {"type": "integer"},
                    "release_date": {"type": "string", "format": "date-time"},
                    "planned_duration": {"type": "string"},
                    "due_date": {"type": "string", "format": "date-time"},
                },
                "required": ["operation"],
            },
            "CancelOperationRequest": {
                "type": "object",
                "properties": {
                    "operation_id": {
                        "type": "string",
                        "format": "uuid",
                    }
                },
                "required": ["operation_id"],
            },
        }

        definitions.update(self._operation_openapi_definitions)
        return definitions

    def _normalize_operation(self, operation_payload: Any) -> Operation:
        validated_operation = self._operation_adapter.validate_python(operation_payload)
        if isinstance(validated_operation, BaseModel):
            return self._operation_model.model_validate(
                validated_operation.model_dump()
            )
        if isinstance(validated_operation, dict):
            return self._operation_model.model_validate(validated_operation)
        raise TypeError("operation model must resolve to an object")

    def _build_operation_openapi_components(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        operation_schema = self._operation_adapter.json_schema()
        operation_definitions: dict[str, Any] = {
            definition_name: self._convert_schema_to_openapi(definition_schema)
            for definition_name, definition_schema in operation_schema.get(
                "$defs", {}
            ).items()
        }

        root_schema = self._convert_schema_to_openapi(
            {key: value for key, value in operation_schema.items() if key != "$defs"}
        )

        if "$ref" in root_schema:
            return {"$ref": root_schema["$ref"]}, operation_definitions

        definition_name = operation_schema.get("title") or "OperationModel"
        root_schema.pop("title", None)
        operation_definitions[definition_name] = root_schema
        return {"$ref": f"#/definitions/{definition_name}"}, operation_definitions

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
