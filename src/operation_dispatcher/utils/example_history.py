from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    ExecutionOutcome,
    ExecutionState,
    History,
    Operation,
    TerminationReason,
)
from operation_dispatcher.models import ChangeRecord


def build_example_history(resource_id: str = "resource-a") -> History:
    """Build a deterministic 4-operation history with dispatcher lifecycle events."""

    base_time = datetime(2026, 6, 10, 8, 0, tzinfo=timezone.utc)

    operation_success_1 = Operation(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        payload={"task": "batch-sync", "target": "erp"},
        resource_id=resource_id,
        priority=8,
        release_date=base_time,
        planned_duration=1800,
        due_date=base_time + timedelta(hours=1),
        state=ExecutionState.COMPLETED,
        outcome=ExecutionOutcome.SUCCESS,
        termination_reason=TerminationReason.NONE,
        retry_count=0,
        start_time=base_time + timedelta(minutes=2),
        finish_time=base_time + timedelta(minutes=20),
        created_at=base_time - timedelta(minutes=2),
    )

    operation_cancelled = Operation(
        id=UUID("22222222-2222-2222-2222-222222222222"),
        payload={"task": "upload-archive", "channel": "s3"},
        resource_id=resource_id,
        priority=5,
        release_date=base_time + timedelta(minutes=21),
        planned_duration=900,
        due_date=base_time + timedelta(hours=2),
        state=ExecutionState.COMPLETED,
        outcome=ExecutionOutcome.CANCELLED,
        termination_reason=TerminationReason.USER_REQUEST,
        retry_count=0,
        start_time=None,
        finish_time=base_time + timedelta(minutes=25),
        created_at=base_time + timedelta(minutes=21),
    )

    operation_paused_resumed = Operation(
        id=UUID("33333333-3333-3333-3333-333333333333"),
        payload={"task": "inventory-reconcile", "warehouse": "north"},
        resource_id=resource_id,
        priority=6,
        release_date=base_time + timedelta(minutes=31),
        planned_duration=2400,
        due_date=base_time + timedelta(hours=3),
        state=ExecutionState.COMPLETED,
        outcome=ExecutionOutcome.SUCCESS,
        termination_reason=TerminationReason.NONE,
        retry_count=0,
        start_time=base_time + timedelta(minutes=32),
        finish_time=base_time + timedelta(minutes=58),
        created_at=base_time + timedelta(minutes=31),
    )

    operation_success_2 = Operation(
        id=UUID("44444444-4444-4444-4444-444444444444"),
        payload={"task": "generate-report", "report": "daily-kpi"},
        resource_id=resource_id,
        priority=4,
        release_date=base_time + timedelta(minutes=59),
        planned_duration=1200,
        due_date=base_time + timedelta(hours=4),
        state=ExecutionState.COMPLETED,
        outcome=ExecutionOutcome.SUCCESS,
        termination_reason=TerminationReason.NONE,
        retry_count=0,
        start_time=base_time + timedelta(minutes=60),
        finish_time=base_time + timedelta(minutes=72),
        created_at=base_time + timedelta(minutes=59),
    )

    events = [
        DispatchEvent(
            id=UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f001"),
            resource_id=resource_id,
            event_type=EventType.OPERATION_DISPATCHER_STARTED,
            created_at=base_time,
            meta_data={"origin": "example_history"},
        ),
        DispatchEvent(
            id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
            resource_id=resource_id,
            operation_id=operation_success_1.id,
            event_type=EventType.OPERATION_ADDED,
            created_at=base_time - timedelta(minutes=2),
            changes=[ChangeRecord(field="state", old_value=None, new_value="QUEUED")],
        ),
        DispatchEvent(
            id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"),
            resource_id=resource_id,
            operation_id=operation_success_1.id,
            event_type=EventType.OPERATION_STARTED,
            created_at=base_time + timedelta(minutes=2),
            changes=[
                ChangeRecord(field="state", old_value="QUEUED", new_value="RUNNING")
            ],
        ),
        DispatchEvent(
            id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3"),
            resource_id=resource_id,
            operation_id=operation_success_1.id,
            event_type=EventType.OPERATION_COMPLETED,
            created_at=base_time + timedelta(minutes=20),
            changes=[
                ChangeRecord(field="state", old_value="RUNNING", new_value="COMPLETED"),
                ChangeRecord(field="outcome", old_value="NONE", new_value="SUCCESS"),
            ],
        ),
        DispatchEvent(
            id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1"),
            resource_id=resource_id,
            operation_id=operation_cancelled.id,
            event_type=EventType.OPERATION_ADDED,
            created_at=base_time + timedelta(minutes=21),
            changes=[ChangeRecord(field="state", old_value=None, new_value="QUEUED")],
        ),
        DispatchEvent(
            id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2"),
            resource_id=resource_id,
            operation_id=operation_cancelled.id,
            event_type=EventType.OPERATION_CANCEL_REQUESTED,
            created_at=base_time + timedelta(minutes=24),
            meta_data={
                "request_decision": {"accepted": True},
                "initiator": "operator",
            },
        ),
        DispatchEvent(
            id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb3"),
            resource_id=resource_id,
            operation_id=operation_cancelled.id,
            event_type=EventType.OPERATION_CANCELLED,
            created_at=base_time + timedelta(minutes=25),
            changes=[
                ChangeRecord(field="state", old_value="QUEUED", new_value="CANCELLED"),
                ChangeRecord(field="outcome", old_value="NONE", new_value="CANCELLED"),
            ],
        ),
        DispatchEvent(
            id=UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f002"),
            resource_id=resource_id,
            event_type=EventType.OPERATION_DISPATCHER_PAUSED,
            created_at=base_time + timedelta(minutes=26),
            meta_data={"reason": "scheduled_break"},
        ),
        DispatchEvent(
            id=UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f003"),
            resource_id=resource_id,
            event_type=EventType.OPERATION_DISPATCHER_RESUMED,
            created_at=base_time + timedelta(minutes=31),
            meta_data={"reason": "break_completed"},
        ),
        DispatchEvent(
            id=UUID("cccccccc-cccc-cccc-cccc-ccccccccccc1"),
            resource_id=resource_id,
            operation_id=operation_paused_resumed.id,
            event_type=EventType.OPERATION_ADDED,
            created_at=base_time + timedelta(minutes=31),
            changes=[ChangeRecord(field="state", old_value=None, new_value="QUEUED")],
        ),
        DispatchEvent(
            id=UUID("cccccccc-cccc-cccc-cccc-ccccccccccc2"),
            resource_id=resource_id,
            operation_id=operation_paused_resumed.id,
            event_type=EventType.OPERATION_STARTED,
            created_at=base_time + timedelta(minutes=32),
            changes=[
                ChangeRecord(field="state", old_value="QUEUED", new_value="RUNNING")
            ],
        ),
        DispatchEvent(
            id=UUID("cccccccc-cccc-cccc-cccc-ccccccccccc3"),
            resource_id=resource_id,
            operation_id=operation_paused_resumed.id,
            event_type=EventType.OPERATION_PAUSED,
            created_at=base_time + timedelta(minutes=40),
            changes=[
                ChangeRecord(field="state", old_value="RUNNING", new_value="PAUSED")
            ],
        ),
        DispatchEvent(
            id=UUID("cccccccc-cccc-cccc-cccc-ccccccccccc4"),
            resource_id=resource_id,
            operation_id=operation_paused_resumed.id,
            event_type=EventType.OPERATION_RESUMED,
            created_at=base_time + timedelta(minutes=47),
            changes=[
                ChangeRecord(field="state", old_value="PAUSED", new_value="RUNNING")
            ],
        ),
        DispatchEvent(
            id=UUID("cccccccc-cccc-cccc-cccc-ccccccccccc5"),
            resource_id=resource_id,
            operation_id=operation_paused_resumed.id,
            event_type=EventType.OPERATION_COMPLETED,
            created_at=base_time + timedelta(minutes=58),
            changes=[
                ChangeRecord(field="state", old_value="RUNNING", new_value="COMPLETED"),
                ChangeRecord(field="outcome", old_value="NONE", new_value="SUCCESS"),
            ],
        ),
        DispatchEvent(
            id=UUID("dddddddd-dddd-dddd-dddd-ddddddddddd1"),
            resource_id=resource_id,
            operation_id=operation_success_2.id,
            event_type=EventType.OPERATION_ADDED,
            created_at=base_time + timedelta(minutes=59),
            changes=[ChangeRecord(field="state", old_value=None, new_value="QUEUED")],
        ),
        DispatchEvent(
            id=UUID("dddddddd-dddd-dddd-dddd-ddddddddddd2"),
            resource_id=resource_id,
            operation_id=operation_success_2.id,
            event_type=EventType.OPERATION_STARTED,
            created_at=base_time + timedelta(minutes=60),
            changes=[
                ChangeRecord(field="state", old_value="QUEUED", new_value="RUNNING")
            ],
        ),
        DispatchEvent(
            id=UUID("dddddddd-dddd-dddd-dddd-ddddddddddd3"),
            resource_id=resource_id,
            operation_id=operation_success_2.id,
            event_type=EventType.OPERATION_COMPLETED,
            created_at=base_time + timedelta(minutes=72),
            changes=[
                ChangeRecord(field="state", old_value="RUNNING", new_value="COMPLETED"),
                ChangeRecord(field="outcome", old_value="NONE", new_value="SUCCESS"),
            ],
        ),
        DispatchEvent(
            id=UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f004"),
            resource_id=resource_id,
            event_type=EventType.OPERATION_DISPATCHER_STOPPED,
            created_at=base_time + timedelta(minutes=73),
            meta_data={"origin": "example_history"},
        ),
    ]

    return History(
        resource_id=resource_id,
        window={
            "start": base_time - timedelta(minutes=5),
            "end": base_time + timedelta(minutes=73),
        },
        events=events,
        operations=[
            operation_success_1,
            operation_cancelled,
            operation_paused_resumed,
            operation_success_2,
        ],
    )
