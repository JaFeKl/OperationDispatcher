from __future__ import annotations

from collections.abc import Hashable
from datetime import datetime, timedelta


class RetryPolicy:
    def __init__(
        self,
        max_retries: int,
        retry_cooldown_seconds: float,
    ) -> None:
        self._max_retries = max_retries
        self._retry_cooldown_seconds = retry_cooldown_seconds
        self._denial_counts: dict[Hashable, int] = {}
        self._cooldown_until: dict[Hashable, datetime] = {}

    def on_denied(
        self,
        key: Hashable,
        now: datetime,
    ) -> tuple[dict[str, int | float], bool]:
        denial_count = self._denial_counts.get(key, 0) + 1
        self._denial_counts[key] = denial_count

        if self._retry_cooldown_seconds > 0:
            self._cooldown_until[key] = now + timedelta(
                seconds=self._retry_cooldown_seconds
            )

        metadata: dict[str, int | float] = {
            "retry_count": denial_count,
            "max_retries": self._max_retries,
            "cooldown_seconds": self._retry_cooldown_seconds,
        }
        return metadata, denial_count >= self._max_retries

    def is_cooldown_active(self, key: Hashable, now: datetime) -> bool:
        cooldown_until = self._cooldown_until.get(key)
        if cooldown_until is None:
            return False

        if cooldown_until > now:
            return True

        self._cooldown_until.pop(key, None)
        return False

    def cooldown_wait_seconds(self, key: Hashable, now: datetime) -> float:
        cooldown_until = self._cooldown_until.get(key)
        if cooldown_until is None:
            return 0.0

        wait_seconds = (cooldown_until - now).total_seconds()
        if wait_seconds <= 0:
            self._cooldown_until.pop(key, None)
            return 0.0
        return wait_seconds

    def clear(self, key: Hashable) -> None:
        self._denial_counts.pop(key, None)
        self._cooldown_until.pop(key, None)

    def clear_all(self) -> None:
        self._denial_counts.clear()
        self._cooldown_until.clear()
