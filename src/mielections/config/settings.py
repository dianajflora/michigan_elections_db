"""Environment-backed application settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

STREAMLIT_SECRET_LOOKUPS: dict[str, list[tuple[str, str]]] = {
    "DATABASE_URL": [("database", "url")],
    "AUTH_ENABLED": [("auth", "enabled")],
    "ADMIN_APP_USERNAME": [("auth", "admin_username")],
    "ADMIN_APP_PASSWORD": [("auth", "admin_password")],
}


def _read_streamlit_secret(key: str) -> str | None:
    """Read a known key from Streamlit secrets when available."""

    try:
        import streamlit as st
    except Exception:
        return None

    try:
        if key in st.secrets:
            return str(st.secrets[key])

        for section_name, section_key in STREAMLIT_SECRET_LOOKUPS.get(key, []):
            section = st.secrets.get(section_name)
            if section and section_key in section:
                return str(section[section_key])
    except Exception:
        return None

    return None


def get_secret(key: str, default: str | None = None) -> str | None:
    """Read a setting from environment variables or Streamlit secrets."""

    value = os.getenv(key)
    if value not in (None, ""):
        return value

    secret_value = _read_streamlit_secret(key)
    if secret_value not in (None, ""):
        return secret_value

    return default


def get_bool(key: str, default: bool) -> bool:
    """Parse a boolean setting from text."""

    raw_value = get_secret(key)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class AppSettings:
    """Top-level application settings."""

    database_url: str
    app_env: str = "development"
    sql_echo: bool = False
    auth_enabled: bool = True


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Load and cache settings from the current environment."""

    database_url = get_secret("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not configured.")

    return AppSettings(
        database_url=database_url,
        app_env=get_secret("APP_ENV", "development") or "development",
        sql_echo=get_bool("SQL_ECHO", False),
        auth_enabled=get_bool("AUTH_ENABLED", True),
    )


def mask_database_url(database_url: str) -> str:
    """Return a safe-to-display version of the PostgreSQL URL."""

    parsed = urlparse(database_url)
    if not parsed.scheme:
        return "<invalid DATABASE_URL>"

    username = parsed.username or "<user>"
    hostname = parsed.hostname or "<host>"
    port = parsed.port or 5432
    database = parsed.path.lstrip("/") or "<database>"

    return f"{parsed.scheme}://{username}:***@{hostname}:{port}/{database}"
