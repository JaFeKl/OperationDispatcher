from __future__ import annotations

from operation_manager import Operation, Schedule


def main() -> None:
    schedule = Schedule(agent_id="agent-1")

    schedule.add(Operation(name="move_to_station_1", agent_id="agent-1", priority=10))
    schedule.add(Operation(name="move_to_charging", agent_id="agent-1", priority=5))

    print("Queued operations:")
    for operation in schedule.list():
        print(f"- {operation.name} (priority={operation.priority})")

    first = schedule.next()
    if first is not None:
        print(f"\nPulled: {first.name}")
        schedule.complete(first)

    print("\nCompleted history:")
    for operation in schedule.history():
        print(f"- {operation.name} ({operation.lifecycle_status})")


if __name__ == "__main__":
    main()
