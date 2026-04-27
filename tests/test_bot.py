import os
import io
import json
import asyncio
import types
import builtins
import importlib
from pathlib import Path

import pytest


@pytest.fixture()
def temp_files(tmp_path, monkeypatch):
    # Point bot module file paths to temp
    import bot as bot_module
    delayed = tmp_path / "delayed_posts.json"
    status = tmp_path / "posts_status.json"
    last_sent = tmp_path / "last_sent_time.txt"

    monkeypatch.setattr(bot_module, "DELAYED_POSTS_FILE", str(delayed))
    monkeypatch.setattr(bot_module, "POSTS_STATUS_FILE", str(status))
    monkeypatch.setattr(bot_module, "LAST_SENT_FILE", str(last_sent))

    # Ensure clean state
    for p in (delayed, status, last_sent):
        if p.exists():
            p.unlink()

    return bot_module, delayed, status, last_sent


def test_extract_job_id():
    import bot
    assert bot.extract_job_id("https://remotive.com/remote-jobs/software-dev/senior-engineer-2030186") == "2030186"
    assert bot.extract_job_id("https://example.com/foo-bar-12345") == "12345"
    assert bot.extract_job_id("https://example.com/foo-bar-noid") is None


def test_extract_wwr_job_id_stable():
    import bot

    url = "https://weworkremotely.com/remote-jobs/acme-senior-python-engineer"
    job_id_1 = bot.extract_wwr_job_id(url)
    job_id_2 = bot.extract_wwr_job_id(url + "?utm_source=rss")

    assert job_id_1 is not None
    assert job_id_1.startswith("wwr_")
    assert job_id_1 == job_id_2


def test_clean_html():
    import bot
    html = "<p>Hello <b>world</b> &amp; everyone</p>"
    out = bot.clean_html(html)
    # normalize whitespace to be robust to parser spacing
    norm = " ".join(out.split())
    assert norm == "Hello world & everyone"


def test_summarize_description():
    import bot
    txt = "Sentence one. Sentence two. Sentence three."
    assert bot.summarize_description(txt, max_sentences=2) == "Sentence one. Sentence two."
    assert bot.summarize_description("", max_sentences=2) == ""


def test_format_post():
    import bot
    job = {
        "title": "QA Engineer",
        "description": "<p>We need testers. <br/>Great team.</p>",
    }
    link = "https://example.com/apply"
    post = bot.format_post(job, link)
    assert "QA Engineer" in post
    assert "We need testers." in post
    assert "More details: [Apply now](https://example.com/apply)" in post


def test_is_affirmative():
    import bot
    for ok in ("Да", "да", "YES", "yes", "YeS", " yEs "):
        assert bot.is_affirmative(ok) is True
    for no in ("no", "нет", "ok", "post"):
        assert bot.is_affirmative(no) is False


def test_load_save_last_sent_time(temp_files):
    bot, _, _, last_sent = temp_files
    # Save and load
    now = 123456
    bot.save_last_sent_time(now)
    ts = bot.load_last_sent_time()
    assert ts == now


def test_load_save_delayed_posts(temp_files):
    bot, delayed, *_ = temp_files
    items = [{"job_id": "1", "text": "aaa", "added_at": 1}]
    bot.save_delayed_posts(items)
    out = bot.load_delayed_posts()
    assert out == items


def test_load_save_posts_status(temp_files):
    bot, _, status, _ = temp_files
    data = {"1": {"text": "t", "status": "sent", "sent_at": 10}}
    bot.save_posts_status(data)
    out = bot.load_posts_status()
    assert out == data


@pytest.mark.asyncio
async def test_send_post_with_photo(tmp_path, monkeypatch):
    import bot

    # Create dummy banner.png in CWD
    banner = tmp_path / "banner.png"
    banner.write_bytes(b"PNGDATA")
    monkeypatch.chdir(tmp_path)

    # Prepare a fake bot with async send_photo
    class FakeBot:
        def __init__(self):
            self.calls = []
        async def send_photo(self, chat_id, photo, caption, parse_mode):
            # photo is a file-like; read a small chunk to assert it opens
            content = photo.read(4)
            self.calls.append((chat_id, caption, parse_mode, content))

    fake = FakeBot()
    await bot.send_post_with_photo(fake, 123, "hello")
    assert fake.calls and fake.calls[0][0] == 123
    assert fake.calls[0][1] == "hello"
    assert fake.calls[0][2] == "Markdown"


def test_clean_over_limit_delayed(temp_files):
    bot, delayed, *_ = temp_files
    # Prepare delayed posts with lengths 5 and 1200
    posts = [
        {"job_id": "a", "text": "short", "added_at": 1},
        {"job_id": "b", "text": "x" * 1200, "added_at": 2},
    ]
    bot.save_delayed_posts(posts)
    total, deleted = bot.clean_over_limit_delayed(1000)
    assert total == 2
    assert deleted == 1
    out = bot.load_delayed_posts()
    assert len(out) == 1 and out[0]["job_id"] == "a"
