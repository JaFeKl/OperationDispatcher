from __future__ import annotations

from operation_dispatcher import DispatchQueue, Operation, ScheduledOperation


class WarehouseOperation(Operation):
    name: str
    source_station: str
    target_station: str


def main() -> None:
    dispatch_queue = DispatchQueue(resource_id="robot-1")

    dispatch_queue.add(
        ScheduledOperation(
            operation=WarehouseOperation(
                name="move_to_station_1",
                source_station="INBOUND_A",
                target_station="BUFFER_01",
            ),
            resource_id="robot-1",
            priority=10,
        )
    )
    dispatch_queue.add(
        ScheduledOperation(
            operation=WarehouseOperation(
                name="move_to_charging",
                source_station="BUFFER_01",
                target_station="CHARGER_1",
            ),
            resource_id="robot-1",
            priority=5,
        )
    )

    print("Queued scheduled operations:")
    for scheduled_operation in dispatch_queue.list():
        operation = scheduled_operation.operation
        print(
            f"- {operation.name}"
            f" (priority={scheduled_operation.priority}, resource={scheduled_operation.resource_id})"
        )

    first = dispatch_queue.next()
    if first is not None:
        print(f"\nPulled: {first.operation.name}")
        dispatch_queue.complete(first)

    print("\nCompleted history:")
    for scheduled_operation in dispatch_queue.history():
        print(f"- {scheduled_operation.operation.name}")


if __name__ == "__main__":
    main()
