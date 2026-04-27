from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Union


def _parse_int(value: str | None, default: int = 0) -> int:
    try:
        return int((value or "").strip())
    except (TypeError, ValueError):
        return default


def _parse_target_channel(value: str | None) -> Optional[Union[int, str]]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    as_int = _parse_int(cleaned, default=0)
    if as_int != 0:
        return as_int
    return cleaned


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_user_id: int
    target_channel: Union[int, str]
    sqlite_path: str
    banner_path: str
    post_char_limit: int
    send_interval_seconds: int
    request_timeout_seconds: int
    request_retries: int
    retry_base_delay_seconds: float
    retry_jitter_seconds: float
    monitor_interval_seconds: int


class ConfigError(RuntimeError):
    pass


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    load_dotenv()
    bot_token = _required_env("BOT_TOKEN")
    admin_user_id = _parse_int(_required_env("ADMIN_USER_ID"), default=0)
    if admin_user_id <= 0:
        raise ConfigError("ADMIN_USER_ID must be a positive integer")

    target_channel = _parse_target_channel(_required_env("TARGET_CHANNEL"))
    if target_channel is None:
        raise ConfigError("TARGET_CHANNEL must be a valid channel id or username")

    sqlite_path = os.getenv("SQLITE_PATH", "jobs.db").strip() or "jobs.db"
    banner_path = os.getenv("BANNER_PATH", "banner.png").strip() or "banner.png"

    post_char_limit = _parse_int(os.getenv("POST_CHAR_LIMIT"), 1000)
    send_interval_seconds = _parse_int(os.getenv("SEND_INTERVAL_SECONDS"), int(1.5 * 60 * 60))

    request_timeout_seconds = _parse_int(os.getenv("REQUEST_TIMEOUT_SECONDS"), 20)
    request_retries = _parse_int(os.getenv("REQUEST_RETRIES"), 3)
    retry_base_delay_seconds = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "0.5"))
    retry_jitter_seconds = float(os.getenv("RETRY_JITTER_SECONDS", "0.4"))
    monitor_interval_seconds = _parse_int(os.getenv("MONITOR_INTERVAL_SECONDS"), 86400)

    if post_char_limit <= 0:
        raise ConfigError("POST_CHAR_LIMIT must be positive")
    if send_interval_seconds <= 0:
        raise ConfigError("SEND_INTERVAL_SECONDS must be positive")
    if request_timeout_seconds <= 0:
        raise ConfigError("REQUEST_TIMEOUT_SECONDS must be positive")
    if request_retries <= 0:
        raise ConfigError("REQUEST_RETRIES must be positive")
    if retry_base_delay_seconds < 0:
        raise ConfigError("RETRY_BASE_DELAY_SECONDS must be >= 0")
    if retry_jitter_seconds < 0:
        raise ConfigError("RETRY_JITTER_SECONDS must be >= 0")

    return Settings(
        bot_token=bot_token,
        admin_user_id=admin_user_id,
        target_channel=target_channel,
        sqlite_path=sqlite_path,
        banner_path=banner_path,
        post_char_limit=post_char_limit,
        send_interval_seconds=send_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        request_retries=request_retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
        monitor_interval_seconds=monitor_interval_seconds,
    )
