from __future__ import annotations

import asyncio
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    OperationDispatcher,
    ScheduledOperation,
    SimulatedOperationRunner,
)


class CallbackDrivenDispatcher:
    def __init__(self) -> None:
        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
        )

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self.on_request,
            on_notification_callback=self.on_notification,
            poll_interval_seconds=0.05,
        )

    def on_request(self, event: DispatchEvent) -> bool | None:
        scheduled_operation = self.operation_dispatcher.get_scheduled_operation(
            event.operation_id
        )
        if scheduled_operation is None:
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            run_seconds = float(scheduled_operation.payload.get("run_seconds", 1.0))
            self._simulated_runner.start(
                operation_id=scheduled_operation.id,
                run_seconds=run_seconds,
            )
            print("Started operation with payload:", scheduled_operation.payload)
            return True
        return None

    def on_notification(self, event: DispatchEvent) -> None:
        print(f"Received notification: {event.operation_id} {event.event_type}")

    def _on_completed(self, operation_id: UUID) -> None:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is not None and current.id == operation_id:
            self.operation_dispatcher.complete_current()


async def main() -> None:
    dispatcher_demo = CallbackDrivenDispatcher()

    dispatcher_demo.operation_dispatcher.add(
        ScheduledOperation(
            payload={
                "name": "move_to_station_1",
                "task": "pickup",
                "run_seconds": 1.0,
            },
            resource_id="robot-1",
            priority=10,
        )
    )
    dispatcher_demo.operation_dispatcher.add(
        ScheduledOperation(
            payload={
                "name": "move_to_charging",
                "task": "charge",
                "run_seconds": 1.0,
            },
            resource_id="robot-1",
            priority=5,
        )
    )

    runtime_task = asyncio.create_task(dispatcher_demo.operation_dispatcher.run())
    await asyncio.sleep(3.0)  # allow time for operations to be processed
    dispatcher_demo.operation_dispatcher.request_stop()
    await runtime_task

    print("\nCompleted operations:")
    for (
        scheduled_operation
    ) in dispatcher_demo.operation_dispatcher.dispatch_queue.history():
        print(f"- {scheduled_operation.payload.get('name', 'unknown')}")


if __name__ == "__main__":
    asyncio.run(main())
