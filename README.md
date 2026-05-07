# Operation Scheduler

## Introduction

Operation Scheduler is a lightweight Python library for managing and executing operations for agents or virtual representations.

It gives you three layers:

1. `Schedule`: a priority-ordered queue with history tracking.
2. `Scheduler`: runtime execution and lifecycle control (start, complete, fail, stop, cancel, pause, resume).
3. `SchedulerOpenAPI`: a Flask + Flasgger adapter to expose the scheduler through documented HTTP endpoints.

This project is useful when you need a small, embeddable scheduler component that can run standalone in Python code or be integrated into a larger service/API.

## Installation

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

For API integration:

```bash
pip install -e .[api]
```

## Schedule Model

A `Schedule` is a priority queue with history tracking.

### Operation types

- **All operations** use `Operation`.
- Optional scheduling windows are expressed via `time_window` (`TimeWindow(start, end)`).

All operations share status fields like `lifecycle_status`, `execution_outcome`, `termination_reason`, plus execution timestamps (`start_time`, `finish_time`).

### Queue behavior

- The first added operation locks the queue type:
	- plain queue: operations must not have `time_window`
	- windowed queue: operations must have `time_window`
- Mixed queues are rejected (`Operation` entries must match the locked queue type).
- Plain queues are ordered by `priority` (higher value first).
- Windowed queues are ordered by `time_window.start`, then `priority`.
- `peek()` returns the next queued operation without removing it.
- `next()` pulls the first queued operation, marks it as running, and sets `start_time`.
- Completed operations are available through history (`history(limit=...)`, newest first).

## Example 1: Plain Schedule

Use `Schedule` when you only want queue behavior and do not need runtime execution.

```python
from operation_scheduler import Operation, Schedule

schedule = Schedule(agent_id="agent-1")

schedule.add(Operation(name="collect_metrics", agent_id="agent-1", priority=10))
schedule.add(Operation(name="check_battery", agent_id="agent-1", priority=5))

next_operation = schedule.peek()  # highest-priority next item
queued_operations = schedule.list()

print(next_operation)
print(len(queued_operations))
```

## Example 2: Scheduler Runtime

Use `Scheduler` when operations should execute automatically in an async loop.

```python
import asyncio
from operation_scheduler import Operation, Scheduler, SchedulerEventType


def on_event(event):
	if event.event_type is SchedulerEventType.OPERATION_START_REQUESTED:
		# higher-level admission control
		return True

	if event.event_type is SchedulerEventType.OPERATION_START_DISPATCH_REQUESTED:
		print(f"Dispatch operation {event.operation_name} ({event.operation_id})")
		return True

	if event.event_type is SchedulerEventType.OPERATION_CANCELLED:
		print(f"Cancel operation {event.operation_name} ({event.operation_id})")


async def main() -> None:
	scheduler = Scheduler(
		agent_id="agent-1",
		on_event_callback=on_event,
		poll_interval_seconds=0.1,
	)

	scheduler.add(Operation(name="collect_metrics", agent_id="agent-1", priority=10))
	scheduler.add(Operation(name="check_battery", agent_id="agent-1", priority=5))

	runtime_task = asyncio.create_task(scheduler.run())
	await asyncio.sleep(2)

	scheduler.request_stop()
	await runtime_task


asyncio.run(main())
```

## Example 3: Scheduler with API (Flask + Flasgger)

Use `SchedulerOpenAPI` when you want HTTP endpoints plus Swagger UI/OpenAPI definitions.

```python
from flask import Flask
from flasgger import Swagger
from pydantic import BaseModel

from operation_scheduler import Scheduler, SchedulerOpenAPI


class OperationPayloadModel(BaseModel):
	task: str
	retries: int = 0


app = Flask(__name__)

scheduler = Scheduler(agent_id="agent-1", payload_model=OperationPayloadModel)
scheduler_api = SchedulerOpenAPI(scheduler)

swagger_template = {
	"swagger": "2.0",
	"info": {"title": "Operation Scheduler API", "version": "1.0.0"},
	"definitions": scheduler_api.get_openapi_definitions(),
}
Swagger(app, template=swagger_template)

scheduler_api.register_default_endpoints(app)
```

`Scheduler.run_once()` emits `OPERATION_START_REQUESTED` as a handshake before an operation can start.
Then it emits `OPERATION_START_DISPATCH_REQUESTED` so the higher-level system can trigger the actual start.
If `on_event_callback` returns `False` for either event, the operation stays queued.

When a `payload_model` is configured, `AddOperationRequest.payload` and `ScheduledOperation.payload`
are represented as a real OpenAPI model reference (`$ref`) in `definitions`.

### Default API endpoints

- `GET /scheduler/schedule`
- `GET /scheduler/history` (supports `limit`, default `50`, max `1000`)
- `GET /scheduler/current_operation`
- `GET /scheduler/next_operation`
- `POST /scheduler/add_operation`
- `POST /scheduler/cancel_operation`
- `GET /scheduler/state`
- `POST /scheduler/start`
- `POST /scheduler/stop`

## Included example app

```bash
python examples/flask_scheduler_app.py
```

Then open:

- `http://localhost:8000/docs/`
- `http://localhost:8000/openapi.json`
