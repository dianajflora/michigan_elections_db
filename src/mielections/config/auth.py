"""Streamlit authentication helpers."""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from mielections.config.settings import get_secret, get_settings


@dataclass(frozen=True)
class AppCredentials:
    """Simple username and password pair for an app scope."""

    username: str | None
    password: str | None


def get_app_credentials(scope: str) -> AppCredentials:
    """Load credentials for the requested app scope."""

    prefix = scope.upper()
    return AppCredentials(
        username=get_secret(f"{prefix}_APP_USERNAME"),
        password=get_secret(f"{prefix}_APP_PASSWORD"),
    )


def login_gate(scope: str) -> bool:
    """Render a simple internal login gate for Streamlit apps."""

    settings = get_settings()
    if not settings.auth_enabled:
        return True

    credentials = get_app_credentials(scope)
    if not credentials.password:
        st.info("Authentication is enabled globally, but no app password is configured. Access is temporarily open.")
        return True

    state_key = f"{scope}_authenticated"
    if st.session_state.get(state_key):
        return True

    st.subheader("Login")
    username = None if scope == "query" else st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Sign in", type="primary"):
        username_matches = credentials.username is None or username == credentials.username
        password_matches = password == credentials.password
        if username_matches and password_matches:
            st.session_state[state_key] = True
            st.rerun()
        st.error("Invalid credentials.")

    return False
