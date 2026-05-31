from __future__ import annotations

import asyncio
import logging
import time
from uuid import UUID

from operation_dispatcher import (
    BrowserEventVisualizer,
    DispatchEvent,
    EventType,
    OperationDispatcher,
    Operation,
    SimulatedOperationRunner,
)


class VisualizedDispatcherDemo:
    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self.host = host

        self._start_accept_delay_seconds = 2.0

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=self._logger,
        )

        self.visualizer = BrowserEventVisualizer(
            host=self.host,
            port=8765,
            operation_dispatcher=self.operation_dispatcher,
        )
        self.visualizer.start()

    def _on_request(self, event: DispatchEvent) -> bool | None:
        self.visualizer.on_request(event)

        if event.operation_id is None:
            return None

        operation = self.operation_dispatcher.get_operation(event.operation_id)
        if operation is None:
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            if self._start_accept_delay_seconds > 0:
                time.sleep(self._start_accept_delay_seconds)
            self._simulated_runner.start(
                operation_id=operation.id,
                run_seconds=float(operation.payload.get("run_seconds", 2.0)),
            )
            return True
        return None

    def _on_notification(self, event: DispatchEvent) -> None:
        self.visualizer.on_notification(event)

    def _on_completed(self, operation_id: UUID) -> None:
        self.operation_dispatcher.complete_operation(operation_id)

    async def run_demo(self) -> None:
        self._logger.info(f"Event visualizer running at {self.visualizer.url}")
        self._logger.info(f"Starting in 10 seconds...")
        await asyncio.sleep(10.0)
        self._logger.info(f"Starting simulated demo operations...")

        self.operation_dispatcher.add_operation(
            Operation(
                payload={"name": "pickup", "run_seconds": 5.0},
                resource_id="robot-1",
                priority=10,
            )
        )
        self.operation_dispatcher.add_operation(
            Operation(
                payload={"name": "dropoff", "run_seconds": 5.0},
                resource_id="robot-1",
                priority=5,
            )
        )

        runtime_task = asyncio.create_task(self.operation_dispatcher.run())
        await asyncio.sleep(30.0)
        self.operation_dispatcher.request_stop()
        await runtime_task
        self._logger.info(f"Finished...")

    def shutdown(self) -> None:
        self._simulated_runner.cancel()
        self.visualizer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    demo = VisualizedDispatcherDemo(host="0.0.0.0", logger=logger)
    try:
        await demo.run_demo()
    finally:
        demo.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
