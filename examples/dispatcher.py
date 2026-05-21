from __future__ import annotations

import asyncio
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    OperationDispatcher,
    ScheduledOperation,
)


class CallbackDrivenDispatcher:
    def __init__(self) -> None:
        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self.on_request,
            on_notification_callback=self.on_notification,
            poll_interval_seconds=0.05,
        )

    def on_request(self, event: DispatchEvent) -> bool | None:
        if event.event_type is EventType.OPERATION_START_REQUESTED:
            return True
        return None

    def on_notification(self, event: DispatchEvent) -> None:
        print(f"NOTIFY: {event.operation_id} {event.event_type}")
        if event.event_type is EventType.OPERATION_STARTED:
            if event.operation_id is not None:
                asyncio.create_task(
                    self._complete_operation_after_delay(event.operation_id)
                )

    async def _complete_operation_after_delay(self, operation_id: UUID) -> None:
        await asyncio.sleep(0.2)
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
            },
            resource_id="robot-1",
            priority=5,
        )
    )

    runtime_task = asyncio.create_task(dispatcher_demo.operation_dispatcher.run())
    await asyncio.sleep(1.0)

    dispatcher_demo.operation_dispatcher.request_stop()
    await runtime_task

    print("\nCompleted operations:")
    for (
        scheduled_operation
    ) in dispatcher_demo.operation_dispatcher.dispatch_queue.history():
        print(f"- {scheduled_operation.payload.get('name', 'unknown')}")


if __name__ == "__main__":
    asyncio.run(main())
