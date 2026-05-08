# Operation Manager

## Introduction

Operation Manager is a lightweight Python library for managing and executing operations.

In a shopfloor environment, each asset can have its own independent operation schedule.
An asset could be a mobile robot, a conveyor segment, a test station, or any other controllable unit.

Each asset receives an ordered list of operations that defines what it should do next.

As a concrete example, consider a warehouse mobile robot moving pallets between inbound, storage, and outbound areas.
An **operation** is one unit of work for that robot, for example:

- `pickup_pallet` from station A
- `move_to_buffer_zone` with a target waypoint
- `dropoff_pallet` at station B
- `go_charge` when battery is below threshold

An operation **schedule** is the asset's ordered operation backlog, defining *what should run next*.

Operation Manager includes three layers:

1. `Schedule`: a priority-ordered queue with history tracking.
2. `OperationManager`: runtime execution and lifecycle control (start, complete, fail, stop, cancel, pause, resume).
3. `OperationManagerOpenAPI`: a Flask + Flasgger adapter to expose the operation manager through documented HTTP endpoints.

This project is useful when you need a small, embeddable operation manager component that can run standalone in Python code or be integrated into a larger service/API.

## Key Features

- Out-of-the-box runtime execution
- Provide your own operation payload
- Easily integrated through event callbacks
- Default OpenAPI interface
- Request-handshake control with explicit allow/deny semantics
- Retry, cooldown, and auto-pause behavior for denied requests
- Built-in runtime controls (`start`, `stop`, `pause`, `resume`)
- Queue history and runtime state introspection endpoints
- Implemented in pure Python with only `pydantic` as a runtime dependency

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

## Operation Manager

`OperationManager` drives execution by repeatedly checking the schedule in an async loop.
At a high level, each loop iteration does the following:

1. Skip if paused or if an operation is already running.
2. Peek the next queued operation.
3. Verify time-window constraints (if configured).
4. Execute the start handshake (`OPERATION_START_REQUESTED` then `OPERATION_START_DISPATCH_REQUESTED`).
5. Start the operation when both handshake steps are explicitly allowed.

This keeps scheduling and execution control separate: `Schedule` decides order, while `OperationManager` decides when and whether execution may proceed.

### Event callback integration

`OperationManager` now separates callback responsibilities:

- `on_request_callback`: is expected to handle `*_REQUESTED` decision events and must return explicit `True` to allow progress.
- `on_notification_callback`: receives non-request lifecycle/runtime events in a best-effort way.

Typical responsibilities include:

- publishing operation/runtime events to Kafka or another message bus
- writing operation state transitions to a database
- triggering external orchestration logic (e.g., robot middleware, task dispatch services)
- running policy checks for request events (`OPERATION_START_REQUESTED`, `OPERATION_START_DISPATCH_REQUESTED`, `OPERATION_CANCEL_REQUESTED`, `OPERATION_STOP_REQUESTED`, `OPERATION_RESUME_REQUESTED`)

In other words, `OperationManager` provides execution flow and event emission, while the callback connects that flow to your own infrastructure and business logic.

### Events

`OperationManager` is event-driven. Events are emitted for lifecycle transitions, runtime control, and start handshakes.

#### Event groups

- **Runtime events**: `OPERATION_MANAGER_STARTED`, `OPERATION_MANAGER_STOPPED`, `OPERATION_MANAGER_PAUSED`, `OPERATION_MANAGER_RESUMED`
- **Operation lifecycle events**: `OPERATION_ADDED`, `OPERATION_STARTED`, `OPERATION_FAILED`, `OPERATION_STOPPED`, `OPERATION_CANCELLED`, `OPERATION_COMPLETED`
- **Request events**: `OPERATION_START_REQUESTED`, `OPERATION_START_DISPATCH_REQUESTED`, `OPERATION_CANCEL_REQUESTED`, `OPERATION_STOP_REQUESTED`, `OPERATION_RESUME_REQUESTED`
- **Request denied events**: `OPERATION_START_DENIED`, `OPERATION_START_DISPATCH_DENIED`, `OPERATION_CANCEL_DENIED`, `OPERATION_STOP_DENIED`, `OPERATION_RESUME_DENIED`

#### Start handshake behavior

Before an operation starts, `run_once()` emits `OPERATION_START_REQUESTED`.
If `on_request_callback` is configured, it must return explicit `True` to allow progress.
If denied (or not explicitly `True`), a corresponding denied event is emitted:

- `OPERATION_START_DENIED`
- `OPERATION_START_DISPATCH_DENIED`
- `OPERATION_CANCEL_DENIED`
- `OPERATION_STOP_DENIED`
- `OPERATION_RESUME_DENIED`

Then retry behavior is controlled by:

- `start_request_retry_cooldown_seconds`: wait before asking again
- `start_request_max_retries`: max denied attempts before auto-pause

After the start request is allowed, `OPERATION_START_DISPATCH_REQUESTED` is emitted so the higher-level system can dispatch execution.
That dispatch request also expects explicit `True` when a callback is configured.

## Example 1: Plain Schedule

Use `Schedule` when you only want queue behavior and do not need runtime execution.

```python
from operation_manager import Operation, Schedule

schedule = Schedule(agent_id="agent-1")

schedule.add(Operation(name="collect_metrics", agent_id="agent-1", priority=10))
schedule.add(Operation(name="check_battery", agent_id="agent-1", priority=5))

next_operation = schedule.peek()  # highest-priority next item
queued_operations = schedule.list()

print(next_operation)
print(len(queued_operations))
```

## Example 2: Operation Manager Runtime

Use `OperationManager` when operations should execute automatically in an async loop.

```python
import asyncio
from operation_manager import Operation, OperationManager, OperationManagerEventType


def on_request(event):
	# react on emitted request events
	if event.event_type is OperationManagerEventType.OPERATION_START_REQUESTED:
		# higher-level admission control
		return True

	if event.event_type is OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED:
		print(f"Dispatch operation {event.operation_name} ({event.operation_id})")
		return True


def on_notification(event):
	# react on all other types of events
	if event.event_type is OperationManagerEventType.OPERATION_CANCELLED:
		print(f"Cancel operation {event.operation_name} ({event.operation_id})")


async def main() -> None:
	operation_manager = OperationManager(
		agent_id="agent-1",
		on_request_callback=on_request,
		on_notification_callback=on_notification,
		poll_interval_seconds=0.1,
		start_request_max_retries=3,
		start_request_retry_cooldown_seconds=1.0,
	)

	operation_manager.add(Operation(name="collect_metrics", agent_id="agent-1", priority=10))
	operation_manager.add(Operation(name="check_battery", agent_id="agent-1", priority=5))

	runtime_task = asyncio.create_task(operation_manager.run())
	await asyncio.sleep(2)

	operation_manager.request_stop()
	await runtime_task


asyncio.run(main())
```

## Example 3: Operation Manager with API (Flask + Flasgger)

Use `OperationManagerOpenAPI` when you want HTTP endpoints plus Swagger UI/OpenAPI definitions.

```python
from flask import Flask
from flasgger import Swagger
from pydantic import BaseModel

from operation_manager import OperationManager, OperationManagerOpenAPI


class OperationPayloadModel(BaseModel):
	task: str
	retries: int = 0


app = Flask(__name__)

operation_manager = OperationManager(agent_id="agent-1", payload_model=OperationPayloadModel)
operation_manager_api = OperationManagerOpenAPI(operation_manager)

swagger_template = {
	"swagger": "2.0",
	"info": {"title": "Operation Manager API", "version": "1.0.0"},
	"definitions": operation_manager_api.get_openapi_definitions(),
}
Swagger(app, template=swagger_template)

operation_manager_api.register_default_endpoints(app)
```

Start and lifecycle behavior is event-driven; see the **Events** section above for the full handshake and retry semantics.

When a `payload_model` is configured, `AddOperationRequest.payload` and `ScheduledOperation.payload`
are represented as a real OpenAPI model reference (`$ref`) in `definitions`.

### Default API endpoints

- `GET /operation_manager/schedule`
- `GET /operation_manager/history` (supports `limit`, default `50`, max `1000`)
- `GET /operation_manager/current_operation`
- `GET /operation_manager/next_operation`
- `POST /operation_manager/add_operation`
- `POST /operation_manager/cancel_operation`
- `GET /operation_manager/state`
- `POST /operation_manager/start`
- `POST /operation_manager/stop`
- `POST /operation_manager/pause`
- `POST /operation_manager/resume`

## Included example apps

1. Plain Schedule

```bash
python examples/plain_schedule_example.py
```

2. OperationManager with callbacks

```bash
python examples/operation_manager_callbacks_example.py
```

3. OperationManager integrated in OpenAPI app (Flask + Flasgger)

```bash
python examples/flask_operation_manager_app.py
```

Then open:

- `http://localhost:8000/docs/`
- `http://localhost:8000/openapi.json`
