from __future__ import annotations

import logging
from uuid import UUID

from operation_dispatcher import (
    DispatchEvent,
    EventType,
    OperationDispatcher,
    OperationDispatcherMCPServer,
    OperationDispatcherRuntimeController,
    Operation,
    SimulatedOperationRunner,
    DispatcherMCPTools,
    DispatcherMCPResources,
    DispatcherMCPPrompts,
)


class DemoDispatcherMCPService:
    """
    Example service demonstrating a shared `OperationDispatcher` exposed via MCP only.
    """

    def __init__(self, host: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

        self.operation_dispatcher = OperationDispatcher(
            resource_id="robot-1",
            start_paused=True,
            on_request_callback=self._on_request,
            on_notification_callback=self._on_notification,
            logger=self._logger,
        )

        self._runtime_controller = OperationDispatcherRuntimeController(
            self.operation_dispatcher
        )

        self.mcp_server = OperationDispatcherMCPServer(
            self.operation_dispatcher,
            name="Operation Dispatcher MCP Server",
            instructions="This is an example MCP server exposing an OperationDispatcher instance.",
            host=host,
            tools=list(DispatcherMCPTools),
            resources=list(DispatcherMCPResources),
            prompts=list(DispatcherMCPPrompts),
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

        operation = self.operation_dispatcher.get_operation(event.operation_id)
        if operation is None:
            self._logger.warning(
                "received request event for unknown operation_id %s",
                event.operation_id,
            )
            return None

        if event.event_type is EventType.OPERATION_START_REQUESTED:
            return self._simulated_runner.start(
                operation_id=operation.id,
                run_seconds=float(operation.payload.get("run_seconds", 2.0)),
            )

        elif event.event_type is EventType.OPERATION_CANCEL_REQUESTED:
            return self._simulated_runner.cancel(operation_id=operation.id)

        elif event.event_type is EventType.OPERATION_PAUSE_REQUESTED:
            return self._simulated_runner.pause(operation_id=operation.id)

        elif event.event_type is EventType.OPERATION_RESUME_REQUESTED:
            return self._simulated_runner.resume(operation_id=operation.id)

        return None

    def _on_notification(self, event: DispatchEvent) -> None:
        self._logger.info(
            f"Received notification event {event.event_type} for operation_id {event.operation_id}"
        )

    def _on_completed(self, operation_id: UUID) -> None:
        self.operation_dispatcher.complete_operation(operation_id)

    def run_demo(self) -> None:
        self.operation_dispatcher.add_operation(
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
        self.operation_dispatcher.add_operation(
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

        runtime_payload, runtime_status = self._runtime_controller.start()
        if runtime_status >= 400:
            raise RuntimeError(
                f"failed to start operation dispatcher runtime: {runtime_payload}"
            )

        self.mcp_server.run(transport="streamable-http")

    def shutdown(self) -> None:
        self._runtime_controller.stop()
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
    try:
        main()
    except KeyboardInterrupt:
        pass
