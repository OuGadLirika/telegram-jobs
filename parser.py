from __future__ import annotations

import hashlib
import re
import textwrap
from urllib.parse import urlparse

from bs4 import BeautifulSoup


def extract_job_id(url: str) -> str | None:
    parts = urlparse(url).path.strip("/").split("-")
    try:
        return str(int(parts[-1]))
    except (TypeError, ValueError, IndexError):
        return None


def is_weworkremotely_source(source_url: str) -> bool:
    host = urlparse(source_url or "").netloc.lower()
    return "weworkremotely.com" in host


def extract_wwr_job_id(url: str) -> str | None:
    parsed = urlparse(url or "")
    if not parsed.netloc:
        return None
    normalized = f"https://{parsed.netloc.lower()}{parsed.path}".rstrip("/")
    if not normalized:
        return None
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return f"wwr_{digest}"


def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html or "", "html.parser")
    return soup.get_text(separator=" ")


def summarize_description(text: str, max_sentences: int = 2) -> str:
    sentences = [s.strip() for s in (text or "").replace("\n", " ").split(".") if s.strip()]
    return ". ".join(sentences[:max_sentences]) + "." if sentences else ""


def normalize_text(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered


def job_fingerprint(title: str, company: str, apply_link: str, location: str) -> str:
    payload = "|".join(
        [
            normalize_text(title),
            normalize_text(company),
            normalize_text(apply_link),
            normalize_text(location),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def format_post(job: dict[str, str], link: str) -> str:
    title = job.get("title", "Untitled")
    description_html = job.get("description", "")
    description_text = clean_html(description_html)
    short_desc = summarize_description(description_text)
    return textwrap.dedent(
        f"""
        🌍 {title} - Remote ✅

        {short_desc}

        👤 More details: [Apply now]({link})
        ➡️ Post your vacancy: @seevov
        """
    ).strip()


def extract_job_id_from_message_text(message_text: str | None) -> str | None:
    if not message_text:
        return None
    lines = message_text.splitlines()
    for line in reversed(lines):
        if line.strip().startswith("ID:"):
            return line.strip().split("ID:", 1)[1].strip()
    return None
