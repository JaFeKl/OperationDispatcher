# Operation Dispatcher

Operation Dispatcher is a lightweight Python package for managing dispatching, execution and supervising of queued operations for a single resource (for example a robot, station, or machine). A typical motivating example is a mobile robot that receives transport jobs, waits until each job is released, asks an external controller whether it may start, and then reports lifecycle updates such as started, paused, stopped, cancelled, or completed.

It is organized in three layers:

1. `DispatchQueue`: ordering, pull, cancel, and completion history.
2. `OperationDispatcher`: runtime loop, request handshake, lifecycle transitions, and execution state.
3. `OperationDispatcherOpenAPI`: Flask + Flasgger adapter exposing documented HTTP endpoints.

## Highlights

- Typed models with Pydantic v2.
- Custom operation schema via `operation_model`.
- Request/notification callback split.
- Retry/cooldown policy for denied start requests.
- Runtime controls: start, stop, pause, resume.
- OpenAPI endpoints for queue/history/current/next/add/cancel/state.

## Installation

Base package:

```bash
pip install -e .
```

With tests/dev tools:

```bash
pip install -e .[dev]
```

With Flask + Flasgger API extras:

```bash
pip install -e .[api]
```

## Core Data Models

- `Operation`: base operation model (subclass this for app-specific fields).
- `ScheduledOperation`: operation + dispatch metadata.
- `OperationExecution`: runtime execution state and outcome metadata, references a ScheduledOperation.
- `DispatchEvent` with `EventType`: emitted for lifecycle/runtime/request flow.

## Dispatch Queue

`DispatchQueue` stores scheduled operations for one `resource_id`.

Key behavior:

- Keeps one active pulled operation at a time.
- Supports `add`, `peek`, `next`, `complete`, `cancel`, `remove`.
- Tracks completion history via `history(limit=...)` (newest first).
- Supports ordering strategies:
  - `SortStrategy.RELEASE_DATE_THEN_PRIORITY` (default)
  - `SortStrategy.PRIORITY_THEN_RELEASE_DATE`

## Dispatcher Runtime

`OperationDispatcher` coordinates queue state, execution state, request/deny handling, and runtime wakeups.

### Inner loop behavior

When `run()` is started, the dispatcher:

1. switches its runtime state to running,
2. stores the current event loop and a wakeup event,
3. emits `OPERATION_MANAGER_STARTED`,
4. repeatedly executes `run_once()` until `request_stop()` is called.

Inside each `run_once()` iteration, the dispatcher performs these checks in order:

1. **Pause / active-operation guard**: if the dispatcher is paused, or if one operation is already pulled and active, the iteration does nothing.
2. **Peek the next queued operation**: the dispatcher looks at the first queue entry without removing it.
3. **Release-date gate**: if that operation has a future `release_date`, the iteration stops and waits until that time.
4. **Retry cooldown gate**: if a previous start request for that operation was denied and the retry cooldown is still active, the iteration stops and waits until the cooldown expires.
5. **Emit and resolve the start request**: the dispatcher creates `OPERATION_START_REQUESTED` and invokes `on_request_callback`.
6. **Handle denial**: if the callback denies the request, the dispatcher records the decision, emits `OPERATION_START_DENIED`, and updates retry state. After the configured number of denied retries is exhausted, the dispatcher pauses itself.
7. **Pull the operation from the queue**: only after approval does the dispatcher call `DispatchQueue.next()`.
8. **Create running execution state**: the matching `OperationExecution` is marked `RUNNING`, and `start_time` is set if it was not already set.
9. **Emit start notification**: the dispatcher emits `OPERATION_STARTED`.

Callbacks:

- `on_request_callback`: handles `*_REQUESTED` events and should return explicit `True` to allow progression.
- `on_notification_callback`: receives non-request events (best effort).

## Event Types

`DispatchEvent` instances are emitted for runtime lifecycle, request handshakes, and operation lifecycle transitions.

### Manager lifecycle events

- `OPERATION_MANAGER_STARTED`: emitted when `run()` enters its main loop.
- `OPERATION_MANAGER_STOPPED`: emitted when the runtime loop exits and dispatcher state is cleaned up.
- `OPERATION_MANAGER_PAUSED`: emitted when the dispatcher is paused manually or after too many denied retries.
- `OPERATION_MANAGER_RESUMED`: emitted after a successful resume and, if applicable, resume request approval.

### Request events

- `OPERATION_START_REQUESTED`: asks whether the next queued operation may begin.
- `OPERATION_START_DENIED`: indicates that a start request was denied; event metadata includes retry information and optionally the denial reason.
- `OPERATION_CANCEL_REQUESTED`: asks whether a queued or active operation may be cancelled.
- `OPERATION_CANCEL_DENIED`: indicates that a cancel request was denied.
- `OPERATION_STOP_REQUESTED`: asks whether the current active operation may be stopped.
- `OPERATION_STOP_DENIED`: indicates that a stop request was denied.
- `OPERATION_RESUME_REQUESTED`: asks whether a paused current operation, or the paused dispatcher, may resume.
- `OPERATION_RESUME_DENIED`: indicates that a resume request was denied.

### Operation lifecycle events

- `OPERATION_ADDED`: emitted when a scheduled operation is inserted into the queue and execution tracking is created.
- `OPERATION_STARTED`: emitted after the operation is pulled from the queue and marked running.
- `OPERATION_COMPLETED`: emitted when the current operation finishes successfully.
- `OPERATION_FAILED`: emitted when the current operation is marked failed because of an internal failure path.
- `OPERATION_STOPPED`: emitted when the current operation is stopped after a successful stop request.
- `OPERATION_CANCELLED`: emitted when an operation is cancelled, whether it was still queued or already active.

### How to interpret them

- `*_REQUESTED` events are the handshake points where business logic decides whether the dispatcher may proceed.
- `*_DENIED` events are feedback that the handshake rejected the action; for start requests they also drive retry/cooldown behavior.
- manager events describe the dispatcher runtime itself,
- operation lifecycle events describe what happened to a specific scheduled operation.

## Quickstart

### 1) Queue only

```python
from operation_dispatcher import DispatchQueue, Operation, ScheduledOperation


class WarehouseOperation(Operation):
	name: str
	source_station: str
	target_station: str


dispatch_queue = DispatchQueue(resource_id="robot-1")
dispatch_queue.add(
	ScheduledOperation(
		operation=WarehouseOperation(
			name="move_to_station_1",
			source_station="INBOUND_A",
			target_station="BUFFER_01",
		),
		resource_id="robot-1",
		priority=10,
	)
)

next_op = dispatch_queue.peek()
print(next_op)
```

### 2) Dispatcher runtime

```python
import asyncio
from operation_dispatcher import DispatchEvent, EventType, Operation, OperationDispatcher, ScheduledOperation


class WarehouseOperation(Operation):
	name: str
	task: str


def on_request(event: DispatchEvent) -> bool | None:
	if event.event_type is EventType.OPERATION_START_REQUESTED:
		return True
	return None


async def main() -> None:
	dispatcher = OperationDispatcher(
		resource_id="robot-1",
		operation_model=WarehouseOperation,
		on_request_callback=on_request,
	)

	dispatcher.add(
		ScheduledOperation(
			operation=WarehouseOperation(name="move", task="pickup"),
			resource_id="robot-1",
			priority=10,
		)
	)

	runtime_task = asyncio.create_task(dispatcher.run())
	await asyncio.sleep(1.0)
	dispatcher.request_stop()
	await runtime_task


asyncio.run(main())
```

### 3) OpenAPI adapter

```python
from flask import Flask
from flasgger import Swagger
from operation_dispatcher import OperationDispatcher, OperationDispatcherOpenAPI


app = Flask(__name__)
dispatcher = OperationDispatcher(resource_id="robot-1")
dispatcher_api = OperationDispatcherOpenAPI(dispatcher)

Swagger(
	app,
	template={
		"swagger": "2.0",
		"info": {"title": "Operation Dispatcher API", "version": "1.0.0"},
		"definitions": dispatcher_api.get_openapi_definitions(),
	},
)

dispatcher_api.register_default_endpoints(app)
```

## Default OpenAPI Endpoints

#### Operation Dispatcher
- `GET /operation_dispatcher/queue`
- `GET /operation_dispatcher/history` (`limit` query parameter, default `50`, max `1000`)
- `GET /operation_dispatcher/next`
- `POST /operation_dispatcher/add`
- `POST /operation_dispatcher/cancel`
- `GET /operation_dispatcher/current_operation`
- `POST /operation_dispatcher/current_operation/cancel`
- `POST /operation_dispatcher/current_operation/stop`
- `POST /operation_dispatcher/current_operation/resume`

#### Operation Dispatcher Runtime

- `GET /operation_dispatcher/state`
- `POST /operation_dispatcher/start`
- `POST /operation_dispatcher/stop`
- `POST /operation_dispatcher/pause`
- `POST /operation_dispatcher/resume`

## Included Examples

- Queue-only example:

```bash
python examples/dispatch_queue.py
```

- Dispatcher callback/runtime example:

```bash
python examples/dispatcher.py
```

- Flask + OpenAPI demo:

```bash
python examples/dispatcher_openapi.py
```

Then open:

- http://localhost:8000/docs/
- http://localhost:8000/openapi.json
