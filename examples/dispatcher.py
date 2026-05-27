from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    OperationDispatcher,
    ScheduledOperation,
    SimulatedOperationRunner,
)


class CallbackDrivenDispatcher:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            poll_interval_seconds=0.05,
            logger=logger,
        )

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=logger,
        )

    def _on_request(self, event: DispatchEvent) -> bool | None:
        self._logger.info(
            f"Received request {event.event_type} for operation_id {event.operation_id}"
        )
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

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            f"Received notification event {event.event_type} for operation_id {event.operation_id}"
        )

    def _on_completed(self, operation_id: UUID) -> None:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is not None and current.id == operation_id:
            self.operation_dispatcher.complete_current()

    async def run_demo(self) -> None:
        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={
                    "name": "my_operation_1",
                    "task": "pickup",
                    "run_seconds": 3.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        self.operation_dispatcher.add(
            ScheduledOperation(
                payload={
                    "name": "my_operation_2",
                    "task": "dropoff",
                    "run_seconds": 1.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        runtime_task = asyncio.create_task(self.operation_dispatcher.run())
        await asyncio.sleep(6.0)  # allow time for operations to be processed
        self.operation_dispatcher.request_stop()
        await runtime_task  # wait for dispatcher to finish shutting down

        print("\nCompleted operations:")
        history = self.operation_dispatcher.get_history()
        for entry in history.entries:
            print(
                f"- {entry.scheduled_operation.id} - {entry.scheduled_operation.payload.get('name', 'unknown')}"
            )

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self.operation_dispatcher.request_stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    dispatcher_demo = CallbackDrivenDispatcher(logger=logger)
    try:
        await dispatcher_demo.run_demo()
    finally:
        dispatcher_demo.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
