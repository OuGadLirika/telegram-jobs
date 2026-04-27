from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class RetryConfig:
    timeout_seconds: int = 20
    retries: int = 3
    base_delay_seconds: float = 0.5
    jitter_seconds: float = 0.4


def request_get_with_retry(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    config: RetryConfig = RetryConfig(),
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(config.retries):
        try:
            return requests.get(url, headers=headers, timeout=config.timeout_seconds)
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= config.retries - 1:
                break
            backoff = config.base_delay_seconds * (2**attempt)
            jitter = random.uniform(0, config.jitter_seconds)
            time.sleep(backoff + jitter)
    if last_error is None:
        raise RuntimeError("request_get_with_retry failed without captured exception")
    raise last_error
