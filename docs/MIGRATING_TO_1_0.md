# Migrating applications to 1.0.0

This guide describes the changes applications need when upgrading from the
0.x versions to 1.0.0.

## Operation ownership and updates

The dispatcher stores a validated deep copy of each added operation. Its reads,
command results, queue view, history, and notification events are detached
snapshots. Do not depend on object identity or expect an earlier returned object
to change when execution advances. Compare IDs and fetch a fresh snapshot:

```python
dispatcher.add_operation(operation)
await dispatcher.step_dispatch()
current = dispatcher.get_operation(operation.id)
print(current.state)
```

Use facade commands to change state. Modifying `dispatcher.dispatch_queue`, an
input operation, or a returned snapshot no longer changes the dispatcher.
Standalone `DispatchQueue` instances remain mutable low-level queues.

`update_operation` accepts only `payload`, `priority`, `release_date`,
`planned_duration`, `due_date`, and `dependencies`. The constructor's
`updatable_fields` can restrict this set. Lifecycle state, outcomes, execution
timestamps, and retry counters cannot be updated directly. Running and completed
operations reject edits; queued and paused operations accept allowed edits.
New operations must be queued with outcome `NONE` and no execution timestamps.
Duplicate IDs are rejected while an operation is queued, active, or archived.

## Runtime pause and operation pause

`pause_dispatcher_runtime()` now only blocks dispatch of new operations. It
leaves active work in its current state. Call `pause_operation(id)` to request
application approval to pause active work, and `resume_operation(id)` to resume
it. Runtime resume clears request retries and permits new dispatches; it does
not change the active operation's state. The application remains responsible
for controlling the physical resource.

## Approval callbacks and concurrency

Return a real boolean, `RequestDecision`, or a dictionary with a boolean
`accepted` field. Strings such as `"false"`, numeric values, missing `accepted`,
and invalid `reasoning`/`data` types deny the request. Import the typed contracts
from the package root:

```python
from operation_dispatcher import RequestDecision

def approve(event):
    return RequestDecision(accepted=True, reasoning="resource ready")
```

Approval runs outside the mutation lock. A callback may read or call the
dispatcher. The dispatcher rechecks the operation after approval: concurrent
completion, cancellation, or edits can invalidate approval. Synchronous commands
raise a conflict (`RuntimeError`); HTTP returns 409 and MCP returns the same
error payload as a tool error. A changed start candidate is not started: a later
dispatch step obtains fresh approval. Accepted request events record decisions,
not guarantees that a transition committed.

`request_max_retries` and `request_retry_cooldown_seconds` are the preferred
constructor settings for start, pause, resume, and cancellation requests. The
old `start_request_*` settings still work. If both names are supplied, the new
setting takes precedence. Remote completion requests are evaluated separately,
with no retry cooldown or dispatcher pause on denial.

Synchronous request callbacks run on worker threads. An async start callback runs
on the runtime loop; async callbacks for synchronous facade commands run on a
worker's event loop. Do not assume callback thread affinity. In async callers,
offload blocking commands, for example
`await asyncio.to_thread(dispatcher.pause_operation, operation_id)`.
A timeout denies the request but cannot terminate application code already
running in a synchronous callback. Keep callbacks bounded and make them decisions;
perform application work in response to committed transition notifications.

Synchronous notifications execute on the thread committing the mutation and
should return promptly. Async notifications are scheduled without awaiting them;
their failures are logged, including failures without a supplied logger. Shutdown
requests stop dispatching and do not wait for notification delivery or stop active
resource work. Use application-owned persistence/queues when delivery guarantees
are required. A runtime-controller stop response can indicate that shutdown is
still pending; inspect the returned state before restarting.

## Remote completion and transport parity

`request_operation_completion(id, outcome, ...)`, HTTP
`POST /operations/<id>/complete`, and MCP `dispatcher_complete_operation` request
approval for `SUCCESS` or `FAILURE`. They require an explicit callback decision.
Local `complete_operation` and `fail_operation` still report execution results
directly. The shared `OperationDispatcherAPI` implements response validation and
payloads for both adapters; custom MCP tools can use it too.

Default MCP tools now cover the normal OpenAPI operations; runtime start/stop
remain opt-in. History limits, validation, and errors match HTTP. MCP `stdio`
does not receive HTTP settings. For HTTP/SSE, the existing `mount_path` argument
selects FastMCP's `path`; passing it with `stdio` raises `ValueError`.
The supported FastMCP range is `>=2.14.7,<5`, tested in CI across major versions
2, 3, and 4. The declared Python range is now `>=3.12,<3.16`; CI covers Python
3.12, 3.13, 3.14, and 3.15.

## History and dependencies

`event_history_limit` configures the total retained event count (default 1,000),
including dispatcher events. HTTP/MCP history query limits follow that value.
Operations are retained separately until explicitly pruned:

```python
removed = dispatcher.prune_completed_operations(keep=100)
```

Pruning keeps the newest completed operations, leaves active/queued work and
events intact, and makes older operation lookups unavailable. History resolution
omits pruned operation records; it still returns retained events. IDs become
available for reuse after pruning, so prefer fresh UUIDs to keep event attribution
unambiguous. None of this storage is durable.

Dependencies continue to be ordering hints among queued operations. Both supported
dependency types place an available provider first; missing providers and failed
providers do not block dispatch. Cycles fall back to configured sorting. Enforce
strict prerequisites in the approval callback. A self-dependency is now rejected
using the owning operation ID, not the dependency record ID.

## Public interfaces

Use `OperationDispatcher`, its model/enumeration types, `RequestDecision`,
`RequestCallback`, `OperationDispatcherAPI`, the Flask/MCP adapters, and
`OperationDispatcherRuntimeController` for integrations. Queue/sort types and
documented diagnostics/visualization utilities remain available.

The `services` package, state store, and underscored attributes are implementation
details. Existing service-class root exports remain for compatibility, but their
constructors are not a stable extension API. The invalid
`OperationDispatcherReference` wildcard export has been removed.
