from __future__ import annotations

from operation_dispatcher import DispatchQueue, ScheduledOperation


def main() -> None:
    dispatch_queue = DispatchQueue(resource_id="robot-1")

    dispatch_queue.add(
        ScheduledOperation(
            payload={
                "name": "my_operation_1",
                "my_custom_field": "hello world",
            },
            resource_id="robot-1",
            priority=10,
        )
    )
    dispatch_queue.add(
        ScheduledOperation(
            payload={
                "name": "my_operation_2",
                "my_custom_field": "goodbye world",
            },
            resource_id="robot-1",
            priority=5,
        )
    )

    print("Queued scheduled operations:")
    for scheduled_operation in dispatch_queue.list():
        print(
            f"- {scheduled_operation.payload.get('name', 'unknown')}"
            f" (priority={scheduled_operation.priority}, resource={scheduled_operation.resource_id})"
        )

    first = dispatch_queue.next()
    if first is not None:
        print(f"\nPulled: {first.payload.get('name', 'unknown')}")
        dispatch_queue.complete(first)

    print("\nCompleted history:")
    for scheduled_operation in dispatch_queue.history():
        print(f"- {scheduled_operation.payload.get('name', 'unknown')}")

    second = dispatch_queue.next()
    if second is not None:
        print(f"\nPulled: {second.payload.get('name', 'unknown')}")
        dispatch_queue.complete(second)

    print("\nCompleted history:")
    for scheduled_operation in dispatch_queue.history():
        print(f"- {scheduled_operation.payload.get('name', 'unknown')}")


if __name__ == "__main__":
    main()
