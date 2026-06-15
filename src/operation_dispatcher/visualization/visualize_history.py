from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from operation_dispatcher.models import (
    EventType,
    ExecutionOutcome,
    ExecutionState,
    History,
    Operation,
)

_DEFAULT_DURATION_SECONDS = 1
_STATUS_COLORS = {
    ExecutionOutcome.SUCCESS.value: "#22c55e",
    ExecutionOutcome.FAILURE.value: "#ef4444",
    ExecutionOutcome.CANCELLED.value: "#f59e0b",
    "PAUSED": "#f97316",
    "DISPATCHER_RUNNING": "#38bdf8",
    "DISPATCHER_PAUSED": "#a78bfa",
}


def _is_remote_session() -> bool:
    return any(
        os.getenv(var_name)
        for var_name in (
            "SSH_CONNECTION",
            "SSH_CLIENT",
            "SSH_TTY",
            "VSCODE_IPC_HOOK_CLI",
        )
    )


def _is_notebook_session() -> bool:
    try:
        from IPython import get_ipython
    except ImportError:
        return False

    shell = get_ipython()
    if shell is None:
        return False

    shell_name = shell.__class__.__name__
    if shell_name == "ZMQInteractiveShell":
        return True

    shell_config = getattr(shell, "config", {})
    return "IPKernelApp" in shell_config


def _operation_label(operation: Operation, index: int) -> str:
    payload = operation.payload
    if "name" in payload and payload["name"] is not None:
        return str(payload["name"])
    if "task" in payload and payload["task"] is not None:
        return str(payload["task"])
    return f"operation-{index}"


def _operation_status(operation: Operation) -> str:
    outcome = operation.outcome
    if outcome is not ExecutionOutcome.NONE:
        return outcome.value
    return operation.state.value


def _operation_window(operation: Operation) -> tuple[Any, Any]:
    start_time = operation.start_time or operation.created_at

    if operation.finish_time is not None:
        finish_time = operation.finish_time
    elif operation.planned_duration is not None:
        finish_time = start_time + timedelta(seconds=operation.planned_duration)
    else:
        finish_time = start_time + timedelta(seconds=_DEFAULT_DURATION_SECONDS)

    if finish_time <= start_time:
        finish_time = start_time + timedelta(seconds=_DEFAULT_DURATION_SECONDS)

    return start_time, finish_time


def _operation_pause_intervals(history: History) -> dict[UUID, list[tuple[Any, Any]]]:
    pause_starts: dict[UUID, Any] = {}
    pause_intervals: dict[UUID, list[tuple[Any, Any]]] = {}
    for event in sorted(history.events, key=lambda item: item.created_at):
        operation_id = event.operation_id
        if operation_id is None:
            continue
        if event.event_type is EventType.OPERATION_PAUSED:
            pause_starts[operation_id] = event.created_at
            continue
        if event.event_type is EventType.OPERATION_RESUMED:
            pause_start = pause_starts.pop(operation_id, None)
            if pause_start is None or event.created_at <= pause_start:
                continue
            pause_intervals.setdefault(operation_id, []).append(
                (pause_start, event.created_at)
            )
    return pause_intervals


def _operation_chart_rows(
    history: History, operations: list[Operation]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pause_intervals = _operation_pause_intervals(history)

    for index, operation in enumerate(operations, start=1):
        task = _operation_label(operation, index)
        start_time, finish_time = _operation_window(operation)
        intervals = pause_intervals.get(operation.id, [])
        if not intervals:
            rows.append(
                {
                    "Task": task,
                    "Start": start_time,
                    "Finish": finish_time,
                    "Status": _operation_status(operation),
                }
            )
            continue

        cursor = start_time
        for pause_start, resume_time in intervals:
            if pause_start > cursor:
                rows.append(
                    {
                        "Task": task,
                        "Start": cursor,
                        "Finish": pause_start,
                        "Status": _operation_status(operation),
                    }
                )
            if resume_time > pause_start:
                rows.append(
                    {
                        "Task": task,
                        "Start": pause_start,
                        "Finish": resume_time,
                        "Status": "PAUSED",
                    }
                )
            cursor = max(cursor, resume_time)

        if finish_time > cursor:
            rows.append(
                {
                    "Task": task,
                    "Start": cursor,
                    "Finish": finish_time,
                    "Status": _operation_status(operation),
                }
            )

    return rows


def _dispatcher_chart_rows(history: History) -> list[dict[str, Any]]:
    lifecycle_events = [
        event
        for event in sorted(history.events, key=lambda item: item.created_at)
        if event.event_type
        in {
            EventType.OPERATION_DISPATCHER_STARTED,
            EventType.OPERATION_DISPATCHER_PAUSED,
            EventType.OPERATION_DISPATCHER_RESUMED,
            EventType.OPERATION_DISPATCHER_STOPPED,
        }
    ]
    if not lifecycle_events:
        return []

    rows: list[dict[str, Any]] = []
    running_start = None
    paused_start = None
    for event in lifecycle_events:
        event_time = event.created_at
        if event.event_type is EventType.OPERATION_DISPATCHER_STARTED:
            running_start = event_time
            paused_start = None
        elif event.event_type is EventType.OPERATION_DISPATCHER_PAUSED:
            if running_start is not None and event_time > running_start:
                rows.append(
                    {
                        "Task": "dispatcher",
                        "Start": running_start,
                        "Finish": event_time,
                        "Status": "DISPATCHER_RUNNING",
                    }
                )
            running_start = None
            paused_start = event_time
        elif event.event_type is EventType.OPERATION_DISPATCHER_RESUMED:
            if paused_start is not None and event_time > paused_start:
                rows.append(
                    {
                        "Task": "dispatcher",
                        "Start": paused_start,
                        "Finish": event_time,
                        "Status": "DISPATCHER_PAUSED",
                    }
                )
            paused_start = None
            running_start = event_time
        elif event.event_type is EventType.OPERATION_DISPATCHER_STOPPED:
            if running_start is not None and event_time > running_start:
                rows.append(
                    {
                        "Task": "dispatcher",
                        "Start": running_start,
                        "Finish": event_time,
                        "Status": "DISPATCHER_RUNNING",
                    }
                )
            elif paused_start is not None and event_time > paused_start:
                rows.append(
                    {
                        "Task": "dispatcher",
                        "Start": paused_start,
                        "Finish": event_time,
                        "Status": "DISPATCHER_PAUSED",
                    }
                )
            running_start = None
            paused_start = None

    window_end = history.window.end
    if window_end is not None:
        if running_start is not None and window_end > running_start:
            rows.append(
                {
                    "Task": "dispatcher",
                    "Start": running_start,
                    "Finish": window_end,
                    "Status": "DISPATCHER_RUNNING",
                }
            )
        elif paused_start is not None and window_end > paused_start:
            rows.append(
                {
                    "Task": "dispatcher",
                    "Start": paused_start,
                    "Finish": window_end,
                    "Status": "DISPATCHER_PAUSED",
                }
            )

    return rows


def _derive_operations_from_events(history: History) -> list[Operation]:
    terminal_events = {
        EventType.OPERATION_COMPLETED: (
            ExecutionState.COMPLETED,
            ExecutionOutcome.SUCCESS,
        ),
        EventType.OPERATION_FAILED: (
            ExecutionState.COMPLETED,
            ExecutionOutcome.FAILURE,
        ),
        EventType.OPERATION_CANCELLED: (
            ExecutionState.COMPLETED,
            ExecutionOutcome.CANCELLED,
        ),
    }
    derived_operations: list[Operation] = []
    event_by_operation_id: dict[UUID, Any] = {}
    for event in history.events:
        if event.operation_id is None or event.event_type not in terminal_events:
            continue
        event_by_operation_id[event.operation_id] = event

    for index, (operation_id, event) in enumerate(
        event_by_operation_id.items(), start=1
    ):
        state_value, outcome_value = terminal_events[event.event_type]
        derived_operations.append(
            Operation(
                id=operation_id,
                payload={"name": f"operation-{index}"},
                resource_id=history.resource_id,
                state=state_value,
                outcome=outcome_value,
                start_time=event.created_at
                - timedelta(seconds=_DEFAULT_DURATION_SECONDS),
                finish_time=event.created_at,
                created_at=event.created_at
                - timedelta(seconds=_DEFAULT_DURATION_SECONDS),
            )
        )
    return derived_operations


def build_history_gantt_figure(
    history: History,
    *,
    title: str = "Operation History",
):
    """Build a Plotly gantt chart figure from operation history records."""

    try:
        import plotly.express as px
        import plotly.graph_objects as go
    except ImportError as import_error:
        raise ImportError(
            "Plotly history visualization requires optional dependency group `vis`. "
            "Install with `pip install -e .[vis]`."
        ) from import_error

    operations = list(history.operations or [])
    if not operations:
        operations = _derive_operations_from_events(history)

    if not operations:
        figure = go.Figure()
        figure.update_layout(
            title=title, xaxis_title="Time (UTC)", yaxis_title="Operation"
        )
        figure.add_annotation(
            text="No operations in history",
            showarrow=False,
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
        )
        return figure

    chart_rows = _operation_chart_rows(history, operations)
    chart_rows.extend(_dispatcher_chart_rows(history))

    figure = px.timeline(
        chart_rows,
        x_start="Start",
        x_end="Finish",
        y="Task",
        color="Status",
        color_discrete_map=_STATUS_COLORS,
        title=title,
    )
    figure.update_yaxes(autorange="reversed", title_text="Operation")
    figure.update_xaxes(title_text="Time (UTC)")
    figure.update_layout(showlegend=True)
    return figure


def show_history_gantt(
    history: History,
    *,
    title: str = "Operation History",
    auto_open: bool = True,
    output_html_path: str | None = None,
    notebook_renderer: str = "plotly_mimetype",
):
    """Render or write a Plotly gantt chart for operation history."""

    figure = build_history_gantt_figure(history, title=title)
    if output_html_path is not None:
        figure.write_html(output_html_path, auto_open=auto_open)
        return figure

    if _is_notebook_session():
        figure.show(renderer=notebook_renderer)
        return figure

    if _is_remote_session():
        fallback_path = Path.cwd() / "history_gantt.html"
        figure.write_html(str(fallback_path), auto_open=False)
        return figure

    figure.show()
    return figure


__all__ = ["build_history_gantt_figure", "show_history_gantt"]
