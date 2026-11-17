# Changelog

## Unreleased — preparation for 1.0.0

### Added

- Application-approved remote completion with success/failure through Python,
  HTTP, and MCP, including request and denial events.
- A shared transport-independent API and default MCP tool parity with OpenAPI.
- Configurable event retention through `OperationDispatcher(event_history_limit=...)`.
- `RequestDecision` and `RequestCallback` public exports, general request retry
  setting names, and explicit completed-operation pruning.
- Artifact build/install CI, optional-dependency test configurations, and
  migration/release instructions.
- Tag-triggered PyPI Trusted Publishing, gated by version validation, the shared
  test matrix, and the `pypi` deployment environment.
- Python 3.12–3.15 package compatibility range and a CI interpreter matrix across
  base, API, and MCP dependency profiles.

### Fixed

- Start approvals can no longer start a different or changed operation; concurrent
  dispatch steps cannot create duplicate start requests.
- Active work no longer causes a busy runtime loop when more work is queued.
- Pause, resume, cancellation, and completion callbacks can reenter the dispatcher
  without waiting on its mutation lock. Conflicting results are preserved.
- Invalid structured decisions no longer approve through truthiness coercion.
- Duplicate operation IDs, lifecycle edits through ordinary updates, and edits to
  completed operations are rejected.
- Public operation/event snapshots cannot mutate dispatcher state, and added
  operations are copied and validated.
- Dispatcher pause gates dispatch without changing active-operation state.
- Self-dependency checks use the operation owner ID.
- Notification failures are logged; timed-out synchronous start callbacks do not
  hold event-loop executor shutdown open indefinitely.
- MCP transport arguments are correct for stdio and HTTP, without retrying
  arbitrary internal `TypeError` exceptions.
- Event visualization renders untrusted data as text, without HTML interpolation.
- Wildcard imports resolve correctly; optional API tests skip when extras are absent.

See [the migration guide](docs/MIGRATING_TO_1_0.md) for intentional behavior changes.

## 0.1.0

Initial package version before the 1.0 stabilization work.
