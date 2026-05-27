from __future__ import annotations

import logging
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherMCPServer,
    Operation,
    SimulatedOperationRunner,
)


class DemoDispatcherMCPService:
    """
    Example service demonstrating a shared `OperationDispatcher` exposed via MCP only.
    """

    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )
        self.mcp_server = OperationDispatcherMCPServer(
            self.operation_dispatcher,
            name="Operation Dispatcher MCP Server",
            instructions="This is an example MCP server exposing an OperationDispatcher instance.",
            host=host,
            json_response=True,
        )

        self._simulated_runner = SimulatedOperationRunner(
            on_complete=self._on_completed,
            logger=self._logger,
        )

    def _on_request(self, event: DispatchEvent) -> bool | None:
        self._logger.info(
            f"Received request {event.event_type} for operation_id {event.operation_id}"
        )

        scheduled_operation = self.operation_dispatcher.get_scheduled_operation(
            event.operation_id
        )
        if scheduled_operation is None:
            self._logger.warning(
                "received request event for unknown operation_id %s",
                event.operation_id,
            )
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            try:
                self._simulated_runner.start(
                    operation_id=scheduled_operation.id,
                    run_seconds=float(
                        scheduled_operation.payload.get("run_seconds", 2.0)
                    ),
                )
                return True
            except RuntimeError as error:
                self._logger.warning(
                    "start request failed for operation %s: %s",
                    event.operation_id,
                    error,
                )
                return False

        if event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            self._simulated_runner.cancel(operation_id=scheduled_operation.id)
            return True

        if event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            return self._simulated_runner.pause(operation_id=scheduled_operation.id)

        if event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            return self._simulated_runner.resume(operation_id=scheduled_operation.id)

        return None

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            f"Received notification event {event.event_type} for operation_id {event.operation_id}"
        )

    def _on_completed(self, operation_id: UUID) -> None:
        current = self.operation_dispatcher.current_scheduled_operation
        if current is None or current.id != operation_id:
            self._logger.warning(
                "simulated completion callback received for non-current operation_id %s",
                operation_id,
            )
            return

        self.operation_dispatcher.complete_current()

    def stop(self) -> None:
        self._simulated_runner.cancel()

    def run_demo(self) -> None:
        self.operation_dispatcher.add(
            Operation(
                payload={
                    "name": "my_operation_1",
                    "task": "pickup",
                    "run_seconds": 10.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        self.operation_dispatcher.add(
            Operation(
                payload={
                    "name": "my_operation_2",
                    "task": "dropoff",
                    "run_seconds": 8.0,
                },
                resource_id="robot-1",
                priority=0,
            )
        )
        self.mcp_server.run(transport="sse")

    def shutdown(self) -> None:
        self._simulated_runner.cancel()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    demo_mcp_dispatcher = DemoDispatcherMCPService(host="0.0.0.0", logger=logger)

    try:
        demo_mcp_dispatcher.run_demo()
    finally:
        demo_mcp_dispatcher.shutdown()


if __name__ == "__main__":
    main()
