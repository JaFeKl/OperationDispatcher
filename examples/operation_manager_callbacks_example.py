from __future__ import annotations

import asyncio

from operation_manager import (
    Operation,
    OperationManager,
    OperationManagerEvent,
    OperationManagerEventType,
)


class CallbackDrivenAgent:
    def __init__(self) -> None:
        self.operation_manager = OperationManager(
            agent_id="agent-1",
            on_request_callback=self.on_request,
            on_notification_callback=self.on_notification,
            poll_interval_seconds=0.05,
        )

    def on_request(self, event: OperationManagerEvent) -> bool | None:
        if event.event_type in {
            OperationManagerEventType.OPERATION_START_REQUESTED,
            OperationManagerEventType.OPERATION_START_DISPATCH_REQUESTED,
        }:
            return True
        return None

    def on_notification(self, event: OperationManagerEvent) -> None:
        print(f"NOTIFY: {event.event_type}")
        if event.event_type is OperationManagerEventType.OPERATION_STARTED:
            asyncio.create_task(self._complete_current_after_delay())

    async def _complete_current_after_delay(self) -> None:
        await asyncio.sleep(0.2)
        if self.operation_manager.current_operation is not None:
            self.operation_manager.complete_current()


async def main() -> None:
    agent = CallbackDrivenAgent()

    agent.operation_manager.add(
        Operation(name="collect_metrics", agent_id="agent-1", priority=10)
    )
    agent.operation_manager.add(
        Operation(name="check_battery", agent_id="agent-1", priority=5)
    )

    runtime_task = asyncio.create_task(agent.operation_manager.run())
    await asyncio.sleep(1.0)

    agent.operation_manager.request_stop()
    await runtime_task

    print("\nCompleted operations:")
    for operation in agent.operation_manager.schedule.history():
        print(f"- {operation.name}")


if __name__ == "__main__":
    asyncio.run(main())
