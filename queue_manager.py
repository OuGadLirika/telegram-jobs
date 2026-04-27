from __future__ import annotations

import time
from dataclasses import dataclass

from storage import SQLiteRepository


@dataclass(frozen=True)
class QueueDecision:
    queued: bool
    reason: str | None = None


def now_ts() -> int:
    return int(time.time())


class QueueService:
    def __init__(self, repo: SQLiteRepository, send_interval_seconds: int) -> None:
        self.repo = repo
        self.send_interval_seconds = send_interval_seconds

    def enqueue(self, *, job_id: str, text: str, fingerprint: str, source: str, added_at: int | None = None) -> QueueDecision:
        ts = added_at if added_at is not None else now_ts()
        # Repository applies rate-limit scheduling based on current queue and last sent item.
        scheduled_at = ts
        ok, reason = self.repo.enqueue_post(
            job_id=job_id,
            text=text,
            added_at=ts,
            scheduled_at=scheduled_at,
            fingerprint=fingerprint,
            source=source,
            send_interval_seconds=self.send_interval_seconds,
        )
        return QueueDecision(queued=ok, reason=reason)
