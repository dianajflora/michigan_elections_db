"""Database engine and session utilities."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from mielections.config.settings import get_settings, set_default_database_url_key
from mielections.db.base import Base

LEGACY_TABLE_DROP_ORDER = ("election_usage", "locations", "jurisdictions", "alembic_version")
FULL_TABLE_DROP_ORDER = (*LEGACY_TABLE_DROP_ORDER, "elections", "counties")
_database_url_key = "DATABASE_URL"


def set_database_url_key(database_url_key: str) -> None:
    """Select which configured database URL future sessions should use."""

    global _database_url_key
    if _database_url_key == database_url_key:
        return

    _database_url_key = database_url_key
    set_default_database_url_key(database_url_key)
    get_engine.cache_clear()
    get_session_factory.cache_clear()


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Create and cache the SQLAlchemy engine."""

    settings = get_settings(_database_url_key)
    return create_engine(
        settings.database_url,
        echo=settings.sql_echo,
        future=True,
        pool_pre_ping=True,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    """Create and cache the SQLAlchemy session factory."""

    return sessionmaker(
        bind=get_engine(),
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        class_=Session,
    )


def get_session() -> Session:
    """Return a new SQLAlchemy session."""

    return get_session_factory()()


def ensure_database_schema() -> None:
    """Create any missing tables for the current ORM schema."""

    import mielections.db.models  # noqa: F401

    Base.metadata.create_all(bind=get_engine())


def rebuild_database_schema(*, preserve_reference_tables: bool = True) -> None:
    """Rebuild the schema without Alembic.

    By default this preserves the unchanged counties and elections tables while
    recreating the legacy-dependent locations and election usage tables.
    """

    import mielections.db.models  # noqa: F401

    engine = get_engine()
    drop_order = LEGACY_TABLE_DROP_ORDER if preserve_reference_tables else FULL_TABLE_DROP_ORDER

    with engine.begin() as connection:
        existing_tables = set(inspect(connection).get_table_names())
        for table_name in drop_order:
            if table_name in existing_tables:
                connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Yield a session and commit or roll back automatically."""

    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
