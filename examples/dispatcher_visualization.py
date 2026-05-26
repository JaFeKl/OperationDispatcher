from __future__ import annotations

import asyncio
import time
from uuid import UUID

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    ScheduledOperation,
    SimulatedOperationRunner,
)


class VisualizedDispatcherDemo:
    def __init__(self) -> None:
        self._start_accept_delay_seconds = 1.0

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
        )
        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            poll_interval_seconds=0.05,
        )
        self.visualizer = BrowserEventVisualizer(
            host="0.0.0.0",
            port=8765,
            operation_dispatcher=self.operation_dispatcher,
        )
        self.visualizer.start()

    def _on_request(self, event: DispatchEvent) -> bool | None:
        self.visualizer.on_request(event)

        scheduled_operation = self.operation_dispatcher.get_scheduled_operation(
            event.operation_id
        )
        if scheduled_operation is None:
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            if self._start_accept_delay_seconds > 0:
                time.sleep(self._start_accept_delay_seconds)
            self._simulated_runner.start(
                operation_id=scheduled_operation.id,
                run_seconds=float(scheduled_operation.payload.get("run_seconds", 2.0)),
            )
            return True

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            self._simulated_runner.cancel(operation_id=scheduled_operation.id)
            return True

        if event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            return self._simulated_runner.pause(operation_id=scheduled_operation.id)

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            return self._simulated_runner.resume(operation_id=scheduled_operation.id)

        return None

    def _on_notification(self, event: DispatchEvent) -> None:
        self.visualizer.on_notification(event)

    def _on_completed(self, operation_id: UUID) -> None:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is not None and current.id == operation_id:
            self.operation_dispatcher.complete_current()

    async def run_demo(self) -> None:
        print(f"Event visualizer running at {self.visualizer.url}")

        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={"name": "pickup", "run_seconds": 5.0},
                resource_id="robot-1",
                priority=10,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={"name": "dropoff", "run_seconds": 5.0},
                resource_id="robot-1",
                priority=5,
            )
        )

        runtime_task = asyncio.create_task(self.operation_dispatcher.run())
        await asyncio.sleep(30.0)
        self.operation_dispatcher.request_stop()
        await runtime_task

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self.visualizer.stop()


async def main() -> None:
    demo = VisualizedDispatcherDemo()
    try:
        await demo.run_demo()
    finally:
        demo.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
