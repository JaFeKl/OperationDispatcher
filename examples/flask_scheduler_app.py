from __future__ import annotations

import asyncio

from flask import Flask, jsonify
from flasgger import Swagger
from pydantic import BaseModel

from operation_scheduler import Operation, Scheduler, SchedulerOpenAPI


class ExampleOperationPayload(BaseModel):
    task: str
    retries: int = 0


async def simulate_operation_execution(operation: Operation) -> None:
    print("Starting execution of operation:", operation.id)
    await asyncio.sleep(30)
    print("Finished execution of operation:", operation.id)


def create_app() -> Flask:
    app = Flask(__name__, static_url_path="/app_static")

    scheduler = Scheduler(
        operation_executor=simulate_operation_execution,
        payload_model=ExampleOperationPayload,
    )
    scheduler_api = SchedulerOpenAPI(
        scheduler,
        agent_id="agent-1",
        payload_model=ExampleOperationPayload,
    )
    scheduler.add(Operation(name="collect_metrics", agent_id="agent-1", priority=10))
    scheduler.add(Operation(name="check_battery", agent_id="agent-1", priority=5))

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
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
