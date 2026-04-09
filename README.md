# Operation Scheduler

Minimal Python project skeleton for scheduling typed operations executed by agents.

## What is included

- `Schedule` class to register, queue, and inspect operations.
- Pydantic-based operation model (`Operation`) that defines operation shape and validation.
- A generic operation payload model for extension in your own domain.
- Basic tests with `pytest`.

## Quick start

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```

## Example

```python
from operation_scheduler import Operation, Schedule

schedule = Schedule()
operation = Operation(name="collect_metrics", agent_id="agent-1")

schedule.add(operation)
next_op = schedule.next()
print(next_op)
```
