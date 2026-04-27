from __future__ import annotations

import argparse

from bs4 import BeautifulSoup

from network import RetryConfig, request_get_with_retry


def extract_direct_apply_link(job_url: str, config: RetryConfig) -> str | None:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = request_get_with_retry(job_url, headers=headers, config=config)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    apply_button = soup.find("a", string=lambda text: text and "Apply for this position" in text)
    if apply_button and apply_button.has_attr("href"):
        return str(apply_button["href"])
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve direct apply link by Remotive job_id")
    parser.add_argument("job_id")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    remotive_url = f"https://remotive.com/remote-jobs/software-dev/{args.job_id}"
    config = RetryConfig(timeout_seconds=args.timeout, retries=args.retries)

    direct_link = extract_direct_apply_link(remotive_url, config)
    print(f"🚀 Прямая ссылка для отклика: {direct_link or 'не найдена'}")


if __name__ == "__main__":
    main()
