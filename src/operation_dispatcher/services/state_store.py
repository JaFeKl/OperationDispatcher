from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from collections.abc import Callable
from uuid import UUID

from operation_dispatcher.dispatch_queue import DispatchQueue
from operation_dispatcher.models import DispatchEvent, History, HistoryRecord
from operation_dispatcher.notification_handler import NotificationHandler
from operation_dispatcher.request_handler import RequestHandler


@dataclass(slots=True)
class DispatcherStateStore:
    dispatch_queue: DispatchQueue
    logger: logging.Logger | None
    poll_interval_seconds: float
    default_planned_duration: int | None
    updatable_fields: frozenset[str]
    on_history_callback: (
        Callable[[int | None, History], History | list[HistoryRecord] | None] | None
    ) = None

    is_paused: bool = False
    stop_requested: bool = False
    running_since: datetime | None = None
    runtime_loop: asyncio.AbstractEventLoop | None = None
    wakeup_event: asyncio.Event | None = None

    events_by_operation_id: dict[UUID, list[DispatchEvent]] = field(
        default_factory=dict
    )
    event_history: list[DispatchEvent] = field(default_factory=list)
    event_history_limit: int = 1000

    notification_handler: NotificationHandler | None = None
    request_handler: RequestHandler | None = None
