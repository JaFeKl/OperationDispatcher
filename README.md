# Operation Dispatcher

Operation Dispatcher is a lightweight Python package for queueing, dispatching, and supervising operations for a single resource (for example a robot, station, or machine).

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

## Core Models

- `Operation`: base operation model (subclass this for app-specific fields).
- `ScheduledOperation`: operation + dispatch metadata (`resource_id`, `priority`, dates).
- `OperationExecution`: runtime execution state and outcome metadata.
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

`OperationDispatcher` coordinates queue + execution state.

Runtime flow (`run_once`):

1. Skip if paused or a current operation already exists.
2. Peek next queued operation.
3. Honor `release_date` if set.
4. Emit and evaluate start request events.
5. Start operation only when request checks are allowed.

Callbacks:

- `on_request_callback`: handles `*_REQUESTED` events and should return explicit `True` to allow progression.
- `on_notification_callback`: receives non-request events (best effort).

Important constructor options:

- `operation_model`
- `poll_interval_seconds`
- `start_request_max_retries`
- `start_request_retry_cooldown_seconds`
- `request_event_timeout_seconds`
- `dispatch_queue_sort_strategy`

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
