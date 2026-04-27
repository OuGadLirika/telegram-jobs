from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class QueueItem:
    id: int
    job_id: str
    text: str
    added_at: int
    scheduled_at: int
    fingerprint: str
    source: str


class SQLiteRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT,
                    company TEXT,
                    category TEXT,
                    description TEXT,
                    url TEXT,
                    published TEXT,
                    location TEXT,
                    fingerprint TEXT,
                    created_at INTEGER NOT NULL,
                    duplicate_of_job_id TEXT,
                    duplicate_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS job_links (
                    job_id TEXT PRIMARY KEY,
                    apply_link TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL UNIQUE,
                    text TEXT NOT NULL,
                    added_at INTEGER NOT NULL,
                    scheduled_at INTEGER NOT NULL,
                    fingerprint TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    posted_at INTEGER,
                    error_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS posts_status (
                    job_id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sent_at INTEGER,
                    updated_at INTEGER NOT NULL,
                    error_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS sources (
                    source TEXT PRIMARY KEY,
                    last_fetch_at INTEGER,
                    last_success_at INTEGER,
                    enabled INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    component TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    details TEXT,
                    job_id TEXT,
                    source TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_fingerprint ON jobs(fingerprint);
                CREATE INDEX IF NOT EXISTS idx_queue_status_scheduled ON queue(status, scheduled_at);
                CREATE INDEX IF NOT EXISTS idx_failures_ts ON failures(ts);
                """
            )

    def migrate_from_json_if_needed(self, base_dir: str) -> bool:
        with self._connect() as conn:
            has_jobs = conn.execute("SELECT COUNT(1) AS c FROM jobs").fetchone()["c"] > 0
            if has_jobs:
                return False

        base = Path(base_dir)
        jobs_file = base / "jobs.json"
        links_file = base / "links.json"
        delayed_file = base / "delayed_posts.json"
        status_file = base / "posts_status.json"

        jobs = self._load_json(jobs_file, default={})
        links = self._load_json(links_file, default={})
        delayed = self._load_json(delayed_file, default=[])
        statuses = self._load_json(status_file, default={})

        if not jobs and not links and not delayed and not statuses:
            return False

        now = int(time.time())
        with self._connect() as conn:
            for job_id, payload in jobs.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO jobs(
                        job_id, source, title, company, category, description, url, published,
                        location, fingerprint, created_at, duplicate_of_job_id, duplicate_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                    """,
                    (
                        str(job_id),
                        "legacy-json",
                        payload.get("title", ""),
                        payload.get("company", ""),
                        payload.get("category", ""),
                        payload.get("description", ""),
                        payload.get("url", ""),
                        payload.get("published", ""),
                        payload.get("location", ""),
                        payload.get("fingerprint", ""),
                        now,
                    ),
                )

            for job_id, link in links.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO job_links(job_id, apply_link, updated_at)
                    VALUES(?, ?, ?)
                    """,
                    (str(job_id), str(link), now),
                )

            for row in delayed:
                job_id = str(row.get("job_id", "")).strip()
                if not job_id:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO queue(job_id, text, added_at, scheduled_at, fingerprint, source, status)
                    VALUES(?, ?, ?, ?, ?, ?, 'queued')
                    """,
                    (
                        job_id,
                        str(row.get("text", "")),
                        int(row.get("added_at", now)),
                        int(row.get("scheduled_at", row.get("added_at", now))),
                        str(row.get("fingerprint", job_id)),
                        "legacy-json",
                    ),
                )

            for job_id, status in statuses.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO posts_status(job_id, text, status, sent_at, updated_at, error_reason)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(job_id),
                        str(status.get("text", "")),
                        str(status.get("status", "unknown")),
                        status.get("sent_at"),
                        now,
                        status.get("error_reason"),
                    ),
                )

        return True

    @staticmethod
    def _load_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return default

    def upsert_job(self, job_id: str, source: str, job_payload: dict[str, Any], fingerprint: str) -> tuple[bool, Optional[str]]:
        now = int(time.time())
        with self._connect() as conn:
            existing = conn.execute("SELECT job_id FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if existing:
                return False, "duplicate_source_job_id"

            existing_fp = conn.execute(
                "SELECT job_id FROM jobs WHERE fingerprint = ? LIMIT 1", (fingerprint,)
            ).fetchone()
            if existing_fp:
                self.record_failure(
                    component="dedup",
                    reason="duplicate_fingerprint",
                    details=f"duplicate_of={existing_fp['job_id']}",
                    job_id=job_id,
                    source=source,
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO jobs(
                        job_id, source, title, company, category, description, url, published,
                        location, fingerprint, created_at, duplicate_of_job_id, duplicate_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        source,
                        job_payload.get("title", ""),
                        job_payload.get("company", ""),
                        job_payload.get("category", ""),
                        job_payload.get("description", ""),
                        job_payload.get("url", ""),
                        job_payload.get("published", ""),
                        job_payload.get("location", ""),
                        fingerprint,
                        now,
                        existing_fp["job_id"],
                        "duplicate_fingerprint",
                    ),
                )
                return False, "duplicate_fingerprint"

            conn.execute(
                """
                INSERT INTO jobs(
                    job_id, source, title, company, category, description, url, published,
                    location, fingerprint, created_at, duplicate_of_job_id, duplicate_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    job_id,
                    source,
                    job_payload.get("title", ""),
                    job_payload.get("company", ""),
                    job_payload.get("category", ""),
                    job_payload.get("description", ""),
                    job_payload.get("url", ""),
                    job_payload.get("published", ""),
                    job_payload.get("location", ""),
                    fingerprint,
                    now,
                ),
            )
            return True, None

    def save_job_link(self, job_id: str, apply_link: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO job_links(job_id, apply_link, updated_at) VALUES (?, ?, ?)",
                (job_id, apply_link, int(time.time())),
            )

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def get_apply_link(self, job_id: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT apply_link FROM job_links WHERE job_id = ?", (job_id,)).fetchone()
            return str(row["apply_link"]) if row else None

    def enqueue_post(
        self,
        job_id: str,
        text: str,
        added_at: int,
        scheduled_at: int,
        fingerprint: str,
        source: str,
        send_interval_seconds: int,
    ) -> tuple[bool, Optional[str]]:
        with self._connect() as conn:
            by_job = conn.execute(
                "SELECT id FROM queue WHERE job_id = ? AND status IN ('queued','processing')", (job_id,)
            ).fetchone()
            if by_job:
                return False, "duplicate_queue_job_id"

            by_fp = conn.execute(
                "SELECT id FROM queue WHERE fingerprint = ? AND status IN ('queued','processing')", (fingerprint,)
            ).fetchone()
            if by_fp:
                return False, "duplicate_queue_fingerprint"

            # Rate-limit scheduling: each next queued post is at least
            # send_interval_seconds after the last queued/sent post.
            last_queued = conn.execute(
                "SELECT MAX(scheduled_at) AS ts FROM queue WHERE status = 'queued'"
            ).fetchone()
            last_posted = conn.execute(
                "SELECT MAX(posted_at) AS ts FROM queue WHERE status = 'posted'"
            ).fetchone()

            schedule_floor = scheduled_at
            if last_queued and last_queued["ts"] is not None:
                schedule_floor = max(schedule_floor, int(last_queued["ts"]) + send_interval_seconds)
            if last_posted and last_posted["ts"] is not None:
                schedule_floor = max(schedule_floor, int(last_posted["ts"]) + send_interval_seconds)

            conn.execute(
                """
                INSERT INTO queue(job_id, text, added_at, scheduled_at, fingerprint, source, status)
                VALUES(?, ?, ?, ?, ?, ?, 'queued')
                """,
                (job_id, text, added_at, schedule_floor, fingerprint, source),
            )
            return True, None

    def queue_items(self) -> list[QueueItem]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, job_id, text, added_at, scheduled_at, fingerprint, source FROM queue WHERE status = 'queued' ORDER BY scheduled_at, id"
            ).fetchall()
            return [QueueItem(**dict(row)) for row in rows]

    def delete_queue_item(self, queue_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM queue WHERE id = ?", (queue_id,))
            return cur.rowcount > 0

    def delete_queue_item_by_job_id(self, job_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM queue WHERE job_id = ?", (job_id,))
            return cur.rowcount > 0

    def pop_next_ready(self, now_ts: int) -> Optional[QueueItem]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, job_id, text, added_at, scheduled_at, fingerprint, source
                FROM queue
                WHERE status = 'queued' AND scheduled_at <= ?
                ORDER BY scheduled_at, id
                LIMIT 1
                """,
                (now_ts,),
            ).fetchone()
            if not row:
                return None
            conn.execute("UPDATE queue SET status = 'processing' WHERE id = ?", (row["id"],))
            return QueueItem(**dict(row))

    def get_last_posted_at(self) -> Optional[int]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(posted_at) AS ts FROM queue WHERE status = 'posted'"
            ).fetchone()
            if not row or row["ts"] is None:
                return None
            return int(row["ts"])

    def reschedule_queue(self, send_interval_seconds: int, base_ts: int) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM queue WHERE status = 'queued' ORDER BY scheduled_at, id"
            ).fetchall()
            if not rows:
                return 0

            last_posted = conn.execute(
                "SELECT MAX(posted_at) AS ts FROM queue WHERE status = 'posted'"
            ).fetchone()
            first_slot = base_ts
            if last_posted and last_posted["ts"] is not None:
                first_slot = max(first_slot, int(last_posted["ts"]) + send_interval_seconds)

            for idx, row in enumerate(rows):
                scheduled_at = first_slot + idx * send_interval_seconds
                conn.execute("UPDATE queue SET scheduled_at = ? WHERE id = ?", (scheduled_at, int(row["id"])))
            return len(rows)

    def get_queued_item_by_id(self, queue_id: int) -> Optional[QueueItem]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, job_id, text, added_at, scheduled_at, fingerprint, source
                FROM queue
                WHERE id = ? AND status = 'queued'
                LIMIT 1
                """,
                (queue_id,),
            ).fetchone()
            return QueueItem(**dict(row)) if row else None

    def get_queued_item_by_job_id(self, job_id: str) -> Optional[QueueItem]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, job_id, text, added_at, scheduled_at, fingerprint, source
                FROM queue
                WHERE job_id = ? AND status = 'queued'
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            return QueueItem(**dict(row)) if row else None

    def mark_queue_processing(self, queue_id: int) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE queue SET status = 'processing' WHERE id = ?", (queue_id,))

    def mark_queue_deleted(self, queue_id: int, reason: str = "deleted_by_admin") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE queue SET status = 'deleted', error_reason = ? WHERE id = ?",
                (reason, queue_id),
            )

    def mark_queue_posted(self, queue_id: int, posted_at: int) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE queue SET status = 'posted', posted_at = ? WHERE id = ?", (posted_at, queue_id))

    def mark_queue_failed(self, queue_id: int, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE queue SET status = 'queued', error_reason = ? WHERE id = ?",
                (reason, queue_id),
            )

    def upsert_post_status(self, job_id: str, text: str, status: str, sent_at: Optional[int], error_reason: Optional[str] = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO posts_status(job_id, text, status, sent_at, updated_at, error_reason)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    text = excluded.text,
                    status = excluded.status,
                    sent_at = excluded.sent_at,
                    updated_at = excluded.updated_at,
                    error_reason = excluded.error_reason
                """,
                (job_id, text, status, sent_at, int(time.time()), error_reason),
            )

    def get_post_status(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM posts_status WHERE job_id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def record_failure(
        self,
        *,
        component: str,
        reason: str,
        details: str,
        job_id: Optional[str] = None,
        source: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO failures(ts, component, reason, details, job_id, source)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (int(time.time()), component, reason, details, job_id, source),
            )

    def upsert_source_fetch(self, source: str, ok: bool, ts: int) -> None:
        with self._connect() as conn:
            existing = conn.execute("SELECT source FROM sources WHERE source = ?", (source,)).fetchone()
            if existing:
                if ok:
                    conn.execute(
                        "UPDATE sources SET last_fetch_at = ?, last_success_at = ? WHERE source = ?",
                        (ts, ts, source),
                    )
                else:
                    conn.execute("UPDATE sources SET last_fetch_at = ? WHERE source = ?", (ts, source))
                return
            conn.execute(
                "INSERT INTO sources(source, last_fetch_at, last_success_at, enabled) VALUES(?, ?, ?, 1)",
                (source, ts, ts if ok else None),
            )
