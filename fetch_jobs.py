from __future__ import annotations

import argparse

import feedparser

from network import RetryConfig, request_get_with_retry
from parser import extract_job_id, job_fingerprint
from storage import SQLiteRepository

RSS_URL = "https://remotive.com/remote-jobs/feed/software-dev"


def fetch_from_rss(config: RetryConfig):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = request_get_with_retry(RSS_URL, headers=headers, config=config)
    response.raise_for_status()
    feed = feedparser.parse(response.text)
    return feed.entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch jobs from RSS into SQLite storage")
    parser.add_argument("--db", default="jobs.db")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    repository = SQLiteRepository(args.db)
    retry = RetryConfig(timeout_seconds=args.timeout, retries=args.retries)

    entries = fetch_from_rss(retry)
    inserted = 0
    skipped = 0

    for entry in entries:
        job_id = extract_job_id(entry.link)
        if not job_id:
            skipped += 1
            continue

        apply_link = entry.link
        fp = job_fingerprint(
            entry.get("title", ""),
            entry.get("company", ""),
            apply_link,
            entry.get("category", ""),
        )
        ok, _reason = repository.upsert_job(
            job_id,
            RSS_URL,
            {
                "title": entry.get("title", ""),
                "company": entry.get("company", ""),
                "category": entry.get("category", ""),
                "description": entry.get("description", ""),
                "url": entry.get("link", ""),
                "published": entry.get("published", ""),
                "location": entry.get("category", ""),
            },
            fp,
        )
        if not ok:
            skipped += 1
            continue

        repository.save_job_link(job_id, apply_link)
        inserted += 1

    print(f"inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()
