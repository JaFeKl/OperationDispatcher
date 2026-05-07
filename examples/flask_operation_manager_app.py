from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel

from operation_manager import (
    Operation,
    OperationManager,
    OperationManagerEventType,
    OperationManagerOpenAPI,
)
from operation_manager.models import OperationManagerEvent


class ExampleOperationPayload(BaseModel):
    instruction: str
    retries: int = 0


class MyExampleAgent:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._running_operation: dict[UUID, asyncio.Task[None]] = {}
        self.operation_manager = OperationManager(
            agent_id="agent-1",
            on_event_callback=self._on_event_handler,
            payload_model=ExampleOperationPayload,
            logger=logger,
        )

        # add some initial operations to the operation manager
        self.operation_manager.add(
            Operation(name="collect_metrics", agent_id="agent-1", priority=10)
        )
        self.operation_manager.add(
            Operation(name="check_battery", agent_id="agent-1", priority=5)
        )

    def _on_event_handler(self, event: OperationManagerEvent) -> bool | None:
        # react on an event emitted by the operation manager, e.g. log it to a database or stream it.
        if event.event_type is OperationManagerEventType.OPERATION_START_REQUESTED:
            return self.can_start_operation(event)

        if (
            event.event_type
            is OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED
        ):
            return self._dispatch_operation_from_event(event)

        if event.event_type is OperationManagerEventType.OPERATION_CANCEL_REQUESTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self.cancel_operation_task(operation_id)

        return None

    def can_start_operation(self, _: OperationManagerEvent) -> bool:
        # central place for higher-level admission checks before an operation starts
        return True

    async def simulate_operation_execution(self, operation: Operation) -> None:
        print("Starting execution of operation:", operation.id)
        await asyncio.sleep(30)
        print("Finished execution of operation:", operation.id)

    def _dispatch_operation_from_event(self, event: OperationManagerEvent) -> bool:
        operation_id = event.operation_id
        if operation_id is None:
            return False

        queued_operation = next(
            (
                operation
                for operation in self.operation_manager.get_schedule()
                if operation.id == operation_id
            ),
            None,
        )
        if queued_operation is None:
            return False

        self._dispatch_operation(queued_operation)
        return True

    def _dispatch_operation(self, operation: Operation) -> None:
        # In a real implementation, this would trigger the actual execution of the operation, e.g. by sending a command to a robot or invoking an external API.
        task = asyncio.create_task(self._run_and_report(operation))
        self._running_operation[operation.id] = task

    async def _run_and_report(self, operation: Operation) -> None:
        try:
            await self.simulate_operation_execution(operation)
            if (
                self.operation_manager.current_operation is not None
                and self.operation_manager.current_operation.id == operation.id
            ):
                self.operation_manager.complete_current()
        except asyncio.CancelledError:
            pass
        except Exception:
            if (
                self.operation_manager.current_operation is not None
                and self.operation_manager.current_operation.id == operation.id
            ):
                self.operation_manager.fail_current()
        finally:
            self._running_operation.pop(operation.id, None)

    def cancel_operation_task(self, operation_id: UUID) -> None:
        # act on a cancellation event
        print("Cancellation requested for operation:", operation_id)
        task = self._running_operation.get(operation_id)
        if task is not None and not task.done():
            task.cancel()

    def create_app(self) -> Flask:
        app = Flask(__name__, static_url_path="/app_static")

        operation_manager_api = OperationManagerOpenAPI(
            self.operation_manager,
        )

        swagger_template = {
            "swagger": "2.0",
            "info": {
                "title": "Operation Manager API",
                "version": "1.0.0",
                "description": "Example Flask API exposing operation manager endpoints.",
            },
            "definitions": operation_manager_api.get_openapi_definitions(),
        }
        swagger_config = {
            "headers": [],
            "specs": [
                {
                    "endpoint": "openapi",
                    "route": "/openapi.json",
                    "rule_filter": lambda rule: True,
                    "model_filter": lambda tag: True,
                }
            ],
            "swagger_ui": True,
            "specs_route": "/docs/",
            "static_url_path": "/flasgger_static",
        }
        Swagger(app, template=swagger_template, config=swagger_config)

        @app.get("/")
        def root() -> tuple:
            return (
                jsonify(
                    {
                        "message": "Operation Manager example API",
                        "docs": "/docs/",
                        "openapi": "/openapi.json",
                        "schedule": "/operation_manager/schedule",
                        "history": "/operation_manager/history",
                        "get_current_operation": "/operation_manager/current_operation",
                        "get_next_operation": "/operation_manager/next_operation",
                        "add_operation": "/operation_manager/add_operation",
                        "cancel_operation": "/operation_manager/cancel_operation",
                        "get_operation_manager_state": "/operation_manager/state",
                        "start": "/operation_manager/start",
                        "stop": "/operation_manager/stop",
                        "pause": "/operation_manager/pause",
                        "resume": "/operation_manager/resume",
                    }
                ),
                200,
            )

        operation_manager_api.register_default_endpoints(app)

        return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    my_agent = MyExampleAgent(logger=logger)
    app = my_agent.create_app()
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
