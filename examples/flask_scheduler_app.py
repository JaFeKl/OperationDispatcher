from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel

from operation_scheduler import (
    Operation,
    Scheduler,
    SchedulerEventType,
    SchedulerOpenAPI,
)
from operation_scheduler.models import SchedulerEvent


class ExampleOperationPayload(BaseModel):
    instruction: str
    retries: int = 0


class MyExampleAgent:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._running_operation: dict[UUID, asyncio.Task[None]] = {}
        self.scheduler = Scheduler(
            agent_id="agent-1",
            on_event_callback=self._on_event_handler,
            payload_model=ExampleOperationPayload,
            logger=logger,
        )

        # add some initial operations to the scheduler
        self.scheduler.add(
            Operation(name="collect_metrics", agent_id="agent-1", priority=10)
        )
        self.scheduler.add(
            Operation(name="check_battery", agent_id="agent-1", priority=5)
        )

    def _on_event_handler(self, event: SchedulerEvent) -> bool | None:
        # react on an event emitted by the scheduler, e.g. log it to a database or stream it.
        if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
            return self.can_start_operation(event)

        if event.event_type is SchedulerEventType.OPERATION_START_DISPATCH_REQUESTED:
            return self._dispatch_operation_from_event(event)

        if event.event_type is SchedulerEventType.OPERATION_CANCEL_REQUESTED:
            operation_id = event.operation_id
            if operation_id is not None:
                self.cancel_operation_task(operation_id)

        return None

    def can_start_operation(self, _: SchedulerEvent) -> bool:
        # central place for higher-level admission checks before an operation starts
        return True

    async def simulate_operation_execution(self, operation: Operation) -> None:
        print("Starting execution of operation:", operation.id)
        await asyncio.sleep(30)
        print("Finished execution of operation:", operation.id)

    def _dispatch_operation_from_event(self, event: SchedulerEvent) -> bool:
        operation_id = event.operation_id
        if operation_id is None:
            return False

        queued_operation = next(
            (
                operation
                for operation in self.scheduler.get_schedule()
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
                self.scheduler.current_operation is not None
                and self.scheduler.current_operation.id == operation.id
            ):
                self.scheduler.complete_current()
        except asyncio.CancelledError:
            pass
        except Exception:
            if (
                self.scheduler.current_operation is not None
                and self.scheduler.current_operation.id == operation.id
            ):
                self.scheduler.fail_current()
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

        scheduler_api = SchedulerOpenAPI(
            self.scheduler,
        )

        swagger_template = {
            "swagger": "2.0",
            "info": {
                "title": "Operation Scheduler API",
                "version": "1.0.0",
                "description": "Example Flask API exposing scheduler endpoints.",
            },
            "definitions": scheduler_api.get_openapi_definitions(),
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
                        "message": "Operation Scheduler example API",
                        "docs": "/docs/",
                        "openapi": "/openapi.json",
                        "schedule": "/scheduler/schedule",
                        "history": "/scheduler/history",
                        "get_current_operation": "/scheduler/current_operation",
                        "get_next_operation": "/scheduler/next_operation",
                        "add_operation": "/scheduler/add_operation",
                        "cancel_operation": "/scheduler/cancel_operation",
                        "get_scheduler_state": "/scheduler/state",
                        "start": "/scheduler/start",
                        "stop": "/scheduler/stop",
                    }
                ),
                200,
            )

        scheduler_api.register_default_endpoints(app)

        return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    my_agent = MyExampleAgent(logger=logger)
    app = my_agent.create_app()
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
