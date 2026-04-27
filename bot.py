from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any

import feedparser
from telegram.ext import Application, CommandHandler, MessageHandler, filters

from config import ConfigError, Settings, load_settings
from handlers import BotHandlers
from network import RetryConfig, request_get_with_retry
from parser import (
    clean_html,
    extract_job_id,
    extract_wwr_job_id,
    format_post,
    is_weworkremotely_source,
    job_fingerprint,
    summarize_description,
)
from publisher import send_post_with_photo as send_post_with_photo_v2
from queue_manager import QueueService, now_ts
from storage import QueueItem, SQLiteRepository

logger = logging.getLogger("jobbot")
logging.basicConfig(level=logging.INFO, format="%(message)s")

RSS_URLS = [
    "https://remotive.com/remote-jobs/feed",
    "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-front-end-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-management-and-finance-jobs.rss",
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
]

# Legacy files are kept for compatibility and migration.
JOBS_FILE = "jobs.json"
LINKS_FILE = "links.json"
DELAYED_POSTS_FILE = "delayed_posts.json"
POSTS_STATUS_FILE = "posts_status.json"
LAST_SENT_FILE = "last_sent_time.txt"
POST_CHAR_LIMIT = 1000
POST_CACHE: dict[Any, Any] = {}
POSTS_STATUS: dict[str, dict[str, Any]] = {}


def log_event(event: str, **fields: Any) -> None:
    payload = {"ts": now_ts(), "event": event}
    payload.update(fields)
    logger.info(json.dumps(payload, ensure_ascii=False))


class BotService:
    def __init__(self, settings: Settings, repository: SQLiteRepository) -> None:
        self.settings = settings
        self.repo = repository
        self.queue = QueueService(repository, settings.send_interval_seconds)
        self.retry_config = RetryConfig(
            timeout_seconds=settings.request_timeout_seconds,
            retries=settings.request_retries,
            base_delay_seconds=settings.retry_base_delay_seconds,
            jitter_seconds=settings.retry_jitter_seconds,
        )

    @staticmethod
    def log_event(event: str, **fields: Any) -> None:
        log_event(event, **fields)

    @staticmethod
    def is_affirmative(text: str) -> bool:
        return is_affirmative(text)

    async def resolve_apply_link(self, job_id: str, fallback_link: str) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "q.py",
                str(job_id),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=45)
            if proc.returncode != 0:
                self.repo.record_failure(
                    component="parser",
                    reason="apply_link_subprocess_failed",
                    details=stderr.decode(errors="ignore").strip() if stderr else "unknown",
                    job_id=job_id,
                    source="q.py",
                )
                return fallback_link
        except Exception as exc:
            self.repo.record_failure(
                component="network",
                reason="apply_link_subprocess_error",
                details=str(exc),
                job_id=job_id,
                source="q.py",
            )
            return fallback_link

        for line in stdout.decode(errors="ignore").splitlines():
            if line.startswith("🚀 Прямая ссылка для отклика:"):
                candidate = line.split(":", 1)[1].strip()
                if candidate and candidate != "не найдена":
                    return candidate
        return fallback_link

    def _fetch_rss_entries_sync(self) -> list[tuple[str, Any]]:
        entries: list[tuple[str, Any]] = []
        headers = {"User-Agent": "Mozilla/5.0"}
        for source_url in RSS_URLS:
            ts = now_ts()
            try:
                response = request_get_with_retry(source_url, headers=headers, config=self.retry_config)
                response.raise_for_status()
                parsed = feedparser.parse(response.text)
                entries.extend((source_url, entry) for entry in parsed.entries)
                self.repo.upsert_source_fetch(source_url, ok=True, ts=ts)
            except Exception as exc:
                self.repo.upsert_source_fetch(source_url, ok=False, ts=ts)
                self.repo.record_failure(
                    component="network",
                    reason="rss_fetch_failed",
                    details=str(exc),
                    source=source_url,
                )
        return entries

    async def monitor_rss(self, app: Application) -> None:
        del app
        log_event("monitor_started")
        while True:
            metrics = await self.run_rss_cycle()

            log_event("rss_cycle", **metrics)
            await asyncio.sleep(self.settings.monitor_interval_seconds)

    async def run_rss_cycle(self) -> dict[str, int]:
        metrics = {"fetched": 0, "skipped": 0, "queued": 0, "failed": 0}
        try:
            source_entries = await asyncio.to_thread(self._fetch_rss_entries_sync)
            metrics["fetched"] = len(source_entries)
            for source, entry in source_entries:
                processed = await self._process_entry(source, entry)
                for key, value in processed.items():
                    metrics[key] = metrics.get(key, 0) + value
        except Exception as exc:
            metrics["failed"] += 1
            self.repo.record_failure(
                component="monitor",
                reason="monitor_cycle_failed",
                details=str(exc),
            )
        return metrics

    async def _process_entry(self, source: str, entry: Any) -> dict[str, int]:
        metrics = {"skipped": 0, "queued": 0, "failed": 0}
        fallback_link = getattr(entry, "link", "")
        if is_weworkremotely_source(source):
            job_id = extract_wwr_job_id(fallback_link)
        else:
            job_id = extract_job_id(fallback_link)
        if not job_id:
            metrics["skipped"] += 1
            self.repo.record_failure(
                component="validation",
                reason="invalid_job_id",
                details=f"entry_link={getattr(entry, 'link', '')}",
                source=source,
            )
            return metrics

        if is_weworkremotely_source(source):
            apply_link = fallback_link
        else:
            apply_link = await self.resolve_apply_link(job_id, fallback_link)
        location = getattr(entry, "region", "") or getattr(entry, "category", "")
        fingerprint = job_fingerprint(
            getattr(entry, "title", ""),
            getattr(entry, "company", ""),
            apply_link,
            location,
        )

        inserted, reason = await asyncio.to_thread(
            self.repo.upsert_job,
            job_id,
            source,
            {
                "title": getattr(entry, "title", ""),
                "company": getattr(entry, "company", ""),
                "category": getattr(entry, "category", ""),
                "description": getattr(entry, "description", ""),
                "url": getattr(entry, "link", ""),
                "published": getattr(entry, "published", ""),
                "location": location,
            },
            fingerprint,
        )
        if not inserted:
            metrics["skipped"] += 1
            log_event("job_skipped", job_id=job_id, reason=reason or "unknown", source=source)
            return metrics

        await asyncio.to_thread(self.repo.save_job_link, job_id, apply_link)
        job = await asyncio.to_thread(self.repo.get_job, job_id)
        if not job:
            metrics["failed"] += 1
            return metrics

        post_text = format_post(job, apply_link)
        decision = await asyncio.to_thread(
            self.queue.enqueue,
            job_id=job_id,
            text=post_text,
            fingerprint=fingerprint,
            source=source,
        )
        if not decision.queued:
            metrics["skipped"] += 1
            self.repo.record_failure(
                component="queue",
                reason=decision.reason or "queue_rejected",
                details=f"job_id={job_id}",
                job_id=job_id,
                source=source,
            )
            return metrics

        await asyncio.to_thread(
            self.repo.upsert_post_status,
            job_id,
            post_text,
            "delayed",
            None,
            None,
        )
        metrics["queued"] += 1
        return metrics

    async def delayed_post_worker(self, app: Application) -> None:
        log_event("queue_worker_started")
        while True:
            try:
                current_ts = now_ts()
                last_posted_at = await asyncio.to_thread(self.repo.get_last_posted_at)
                if last_posted_at is not None:
                    next_allowed_ts = last_posted_at + self.settings.send_interval_seconds
                    if current_ts < next_allowed_ts:
                        await asyncio.sleep(min(5, next_allowed_ts - current_ts))
                        continue

                item = await asyncio.to_thread(self.repo.pop_next_ready, current_ts)
                if not item:
                    await asyncio.sleep(5)
                    continue
                await self._publish_queue_item(app, item)
            except Exception as exc:
                self.repo.record_failure(
                    component="publisher",
                    reason="delayed_worker_failed",
                    details=str(exc),
                )
                await asyncio.sleep(5)

    async def _publish_queue_item(self, app: Application, item: QueueItem) -> None:
        try:
            await send_post_with_photo_v2(app.bot, self.settings.target_channel, item.text, self.settings.banner_path)
            posted_at = now_ts()
            await asyncio.to_thread(self.repo.mark_queue_posted, item.id, posted_at)
            await asyncio.to_thread(self.repo.upsert_post_status, item.job_id, item.text, "sent", posted_at, None)
            log_event("post_sent", posted=1, failed=0, job_id=item.job_id, queue_id=item.id)
        except Exception as exc:
            await asyncio.to_thread(self.repo.mark_queue_failed, item.id, str(exc))
            await asyncio.to_thread(
                self.repo.upsert_post_status,
                item.job_id,
                item.text,
                "failed",
                None,
                str(exc),
            )
            self.repo.record_failure(
                component="telegram",
                reason="send_failed",
                details=str(exc),
                job_id=item.job_id,
                source=item.source,
            )
            log_event("post_failed", posted=0, failed=1, job_id=item.job_id, queue_id=item.id, error=str(exc))


# -------------------- Legacy helpers (kept for backward compatibility/tests) --------------------

def load_json(filepath: str) -> Any:
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as fh:
            try:
                return json.load(fh)
            except json.JSONDecodeError:
                return {} if filepath.endswith(".json") else None
    return {} if filepath.endswith(".json") else None


def save_json(filepath: str, data: Any) -> None:
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def load_delayed_posts() -> list[dict[str, Any]]:
    data = load_json(DELAYED_POSTS_FILE)
    return data if isinstance(data, list) else []


def save_delayed_posts(posts: list[dict[str, Any]]) -> None:
    save_json(DELAYED_POSTS_FILE, posts)


def load_posts_status() -> dict[str, dict[str, Any]]:
    data = load_json(POSTS_STATUS_FILE)
    return data if isinstance(data, dict) else {}


def save_posts_status(data: dict[str, dict[str, Any]]) -> None:
    save_json(POSTS_STATUS_FILE, data)


def load_last_sent_time() -> int:
    if os.path.exists(LAST_SENT_FILE):
        try:
            with open(LAST_SENT_FILE, "r", encoding="utf-8") as fh:
                ts = int(fh.read().strip())
                return ts if ts > 0 else now_ts()
        except Exception:
            return now_ts()
    return now_ts()


def save_last_sent_time(ts: int) -> None:
    with open(LAST_SENT_FILE, "w", encoding="utf-8") as fh:
        fh.write(str(ts))


def clean_over_limit_delayed(limit: int = POST_CHAR_LIMIT) -> tuple[int, int]:
    posts = load_delayed_posts()
    total = len(posts)
    filtered = [p for p in posts if isinstance(p.get("text", ""), str) and len(p.get("text", "")) <= limit]
    deleted = total - len(filtered)
    if deleted > 0:
        save_delayed_posts(filtered)
    return total, deleted


async def send_post_with_photo(bot, chat_id, text: str) -> None:
    await send_post_with_photo_v2(bot, chat_id, text, "banner.png")


def is_affirmative(text: str) -> bool:
    return text.strip().lower() in {"да", "yes"}


async def post_init(application: Application) -> None:
    svc = application.bot_data["service"]
    monitor_task = asyncio.create_task(svc.monitor_rss(application), name="monitor_rss")
    worker_task = asyncio.create_task(svc.delayed_post_worker(application), name="delayed_post_worker")
    application.bot_data["background_tasks"] = [monitor_task, worker_task]


async def post_shutdown(application: Application) -> None:
    tasks = application.bot_data.get("background_tasks", [])
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    global POSTS_STATUS
    try:
        settings = load_settings()
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        logger.error("Create .env from .env.example and set required values before starting.")
        logger.error("Example: cp .env.example .env")
        sys.exit(2)

    repo = SQLiteRepository(settings.sqlite_path)
    migrated = repo.migrate_from_json_if_needed(os.getcwd())
    if migrated:
        log_event("migration_done", from_format="json", to_format="sqlite", db=settings.sqlite_path)

    POSTS_STATUS = load_posts_status()

    app = (
        Application.builder()
        .token(settings.bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    service = BotService(settings, repo)
    handlers = BotHandlers(service)
    app.bot_data["service"] = service
    app.add_handler(CommandHandler("info", handlers.info_handler))
    app.add_handler(CommandHandler("delete", handlers.delete_handler))
    app.add_handler(CommandHandler("push", handlers.push_handler))
    app.add_handler(CommandHandler("fetch", handlers.fetch_handler))
    app.add_handler(CommandHandler("reschedule", handlers.reschedule_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.text_handler))

    log_event("bot_start", target_channel=str(settings.target_channel), sqlite_path=settings.sqlite_path)
    app.run_polling()


if __name__ == "__main__":
    main()

