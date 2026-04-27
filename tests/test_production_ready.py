import json
import sqlite3
from types import SimpleNamespace

import pytest
import requests

import network
from bot import BotService
from config import Settings
from parser import job_fingerprint
from queue_manager import QueueService
from storage import SQLiteRepository


def make_settings(db_path: str) -> Settings:
    return Settings(
        bot_token="token",
        admin_user_id=1,
        target_channel="@channel",
        sqlite_path=db_path,
        banner_path="banner.png",
        post_char_limit=1000,
        send_interval_seconds=60,
        request_timeout_seconds=20,
        request_retries=3,
        retry_base_delay_seconds=0.1,
        retry_jitter_seconds=0.0,
        monitor_interval_seconds=3600,
    )


def test_dedup_by_fingerprint(tmp_path):
    repo = SQLiteRepository(str(tmp_path / "jobs.db"))
    fp = job_fingerprint("Python Dev", "Acme", "https://apply", "Remote")

    inserted_1, reason_1 = repo.upsert_job(
        "100",
        "source-a",
        {"title": "Python Dev", "company": "Acme", "description": "a", "url": "u"},
        fp,
    )
    inserted_2, reason_2 = repo.upsert_job(
        "101",
        "source-b",
        {"title": "Python Dev", "company": "Acme", "description": "b", "url": "u2"},
        fp,
    )

    assert inserted_1 is True and reason_1 is None
    assert inserted_2 is False and reason_2 == "duplicate_fingerprint"

    with sqlite3.connect(str(tmp_path / "jobs.db")) as conn:
        failures = conn.execute("SELECT reason FROM failures").fetchall()
    assert any(row[0] == "duplicate_fingerprint" for row in failures)


def test_queue_scheduling_idempotent_fifo_with_interval(tmp_path, monkeypatch):
    repo = SQLiteRepository(str(tmp_path / "jobs.db"))
    queue = QueueService(repo, send_interval_seconds=60)

    monkeypatch.setattr("queue_manager.now_ts", lambda: 1000)
    first = queue.enqueue(job_id="1", text="a", fingerprint="fp1", source="s")
    second = queue.enqueue(job_id="2", text="b", fingerprint="fp2", source="s")
    dup = queue.enqueue(job_id="2", text="b", fingerprint="fp2", source="s")

    assert first.queued is True
    assert second.queued is True
    assert dup.queued is False
    assert dup.reason == "duplicate_queue_job_id"

    items = repo.queue_items()
    assert len(items) == 2
    # FIFO + interval: first queued item should be published first.
    assert items[0].job_id == "1"
    assert items[0].scheduled_at == 1000
    assert items[1].job_id == "2"
    assert items[1].scheduled_at == 1060


def test_reschedule_queue_uses_last_posted_floor(tmp_path):
    repo = SQLiteRepository(str(tmp_path / "jobs.db"))

    # Create a posted item to establish the persisted last publication timestamp.
    ok_old, _ = repo.enqueue_post(
        job_id="old",
        text="old",
        added_at=900,
        scheduled_at=900,
        fingerprint="fp-old",
        source="s",
        send_interval_seconds=60,
    )
    assert ok_old is True
    old_item = repo.pop_next_ready(900)
    assert old_item is not None
    repo.mark_queue_posted(old_item.id, 1000)

    ok_1, _ = repo.enqueue_post(
        job_id="1",
        text="a",
        added_at=910,
        scheduled_at=910,
        fingerprint="fp1",
        source="s",
        send_interval_seconds=60,
    )
    ok_2, _ = repo.enqueue_post(
        job_id="2",
        text="b",
        added_at=911,
        scheduled_at=911,
        fingerprint="fp2",
        source="s",
        send_interval_seconds=60,
    )
    assert ok_1 is True and ok_2 is True

    total = repo.reschedule_queue(send_interval_seconds=60, base_ts=1005)
    assert total == 2

    items = repo.queue_items()
    assert [item.job_id for item in items] == ["1", "2"]
    assert items[0].scheduled_at == 1060
    assert items[1].scheduled_at == 1120


def test_retry_timeout_backoff(monkeypatch):
    calls = []
    sleeps = []

    class _Resp:
        status_code = 200

    def fake_get(url, headers=None, timeout=None):
        calls.append(timeout)
        if len(calls) < 3:
            raise requests.RequestException("boom")
        return _Resp()

    monkeypatch.setattr(network.requests, "get", fake_get)
    monkeypatch.setattr(network.time, "sleep", lambda v: sleeps.append(v))
    monkeypatch.setattr(network.random, "uniform", lambda _a, _b: 0.0)

    cfg = network.RetryConfig(timeout_seconds=7, retries=3, base_delay_seconds=0.5, jitter_seconds=0.0)
    resp = network.request_get_with_retry("https://example.com", config=cfg)

    assert resp.status_code == 200
    assert calls == [7, 7, 7]
    assert sleeps == [0.5, 1.0]


def test_migration_json_to_sqlite(tmp_path):
    (tmp_path / "jobs.json").write_text(json.dumps({"1": {"title": "Dev", "description": "desc", "url": "u"}}), encoding="utf-8")
    (tmp_path / "links.json").write_text(json.dumps({"1": "https://apply"}), encoding="utf-8")
    (tmp_path / "delayed_posts.json").write_text(
        json.dumps([{"job_id": "1", "text": "post", "added_at": 100}]),
        encoding="utf-8",
    )
    (tmp_path / "posts_status.json").write_text(
        json.dumps({"1": {"text": "post", "status": "sent", "sent_at": 110}}),
        encoding="utf-8",
    )

    repo = SQLiteRepository(str(tmp_path / "jobs.db"))
    migrated = repo.migrate_from_json_if_needed(str(tmp_path))

    assert migrated is True
    assert repo.get_job("1") is not None
    assert repo.get_apply_link("1") == "https://apply"
    assert len(repo.queue_items()) == 1
    assert repo.get_post_status("1")["status"] == "sent"


@pytest.mark.asyncio
async def test_publish_error_handling(tmp_path):
    db = str(tmp_path / "jobs.db")
    repo = SQLiteRepository(db)
    settings = make_settings(db)
    service = BotService(settings, repo)

    ok, _ = repo.enqueue_post(
        job_id="1",
        text="hello",
        added_at=10,
        scheduled_at=10,
        fingerprint="fp1",
        source="test",
        send_interval_seconds=60,
    )
    assert ok is True
    item = repo.pop_next_ready(10)
    assert item is not None

    class FakeBot:
        async def send_photo(self, **_kwargs):
            raise RuntimeError("telegram error")

    fake_app = SimpleNamespace(bot=FakeBot())
    await service._publish_queue_item(fake_app, item)

    status = repo.get_post_status("1")
    assert status is not None
    assert status["status"] == "failed"
    assert "telegram error" in (status.get("error_reason") or "")


@pytest.mark.asyncio
async def test_run_rss_cycle_aggregates_metrics(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    repo = SQLiteRepository(db)
    settings = make_settings(db)
    service = BotService(settings, repo)

    monkeypatch.setattr(
        service,
        "_fetch_rss_entries_sync",
        lambda: [("source-a", SimpleNamespace(link="u1")), ("source-b", SimpleNamespace(link="u2"))],
    )

    calls = []

    async def fake_process(source, _entry):
        calls.append(source)
        if source == "source-a":
            return {"queued": 1, "skipped": 0, "failed": 0}
        return {"queued": 0, "skipped": 1, "failed": 0}

    monkeypatch.setattr(service, "_process_entry", fake_process)

    metrics = await service.run_rss_cycle()

    assert calls == ["source-a", "source-b"]
    assert metrics == {"fetched": 2, "skipped": 1, "queued": 1, "failed": 0}


@pytest.mark.asyncio
async def test_process_entry_weworkremotely_bypasses_apply_resolver(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    repo = SQLiteRepository(db)
    settings = make_settings(db)
    service = BotService(settings, repo)

    async def _fail_if_called(_job_id, _fallback):
        raise AssertionError("resolve_apply_link should not be called for WeWorkRemotely entries")

    monkeypatch.setattr(service, "resolve_apply_link", _fail_if_called)

    entry = SimpleNamespace(
        link="https://weworkremotely.com/remote-jobs/acme-senior-python-engineer",
        title="Acme: Senior Python Engineer",
        company="",
        category="Back-End Programming",
        region="Anywhere in the World",
        description="Great role.",
        published="Mon, 06 Apr 2026 18:03:08 +0000",
    )

    metrics = await service._process_entry(
        "https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss",
        entry,
    )

    assert metrics["queued"] == 1
    assert metrics["failed"] == 0
