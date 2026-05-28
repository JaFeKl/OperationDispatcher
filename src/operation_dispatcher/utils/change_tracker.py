from __future__ import annotations

from pydantic import BaseModel

from operation_dispatcher.models import ChangeRecord


def get_changes(
    old: BaseModel,
    new: BaseModel,
) -> list[ChangeRecord]:
    old_data = old.model_dump()
    new_data = new.model_dump()

    field_names = list(dict.fromkeys([*old_data.keys(), *new_data.keys()]))

    changes: list[ChangeRecord] = []
    for field_name in field_names:
        old_value = old_data.get(field_name)
        new_value = new_data.get(field_name)
        if old_value == new_value:
            continue
        changes.append(
            ChangeRecord(
                field=field_name,
                old_value=old_value,
                new_value=new_value,
            )
        )

    return changes
