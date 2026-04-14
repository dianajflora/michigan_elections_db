"""PostgreSQL upsert helpers."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from mielections.config.table_metadata import TABLE_DEFINITIONS
from mielections.db.models import County, Election, ElectionUsage, Location

MODEL_REGISTRY = {
    "counties": County,
    "locations": Location,
    "elections": Election,
    "election_usage": ElectionUsage,
}


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame into SQL-friendly record dictionaries."""

    cleaned = df.astype(object).where(pd.notna(df), None)
    return cleaned.to_dict(orient="records")


def upsert_rows(table_name: str, df: pd.DataFrame, session: Session) -> int:
    """Upsert rows into PostgreSQL using the configured unique key."""

    if df.empty:
        return 0

    model = MODEL_REGISTRY[table_name]
    table_definition = TABLE_DEFINITIONS[table_name]
    records = dataframe_to_records(df)

    insert_statement = pg_insert(model).values(records)
    updatable_columns = [
        column.name
        for column in model.__table__.columns
        if not column.primary_key and column.name not in table_definition.upsert_columns
    ]

    if updatable_columns:
        update_mapping = {
            column_name: getattr(insert_statement.excluded, column_name)
            for column_name in updatable_columns
        }
        statement = insert_statement.on_conflict_do_update(
            index_elements=list(table_definition.upsert_columns),
            set_=update_mapping,
        )
    else:
        statement = insert_statement.on_conflict_do_nothing(
            index_elements=list(table_definition.upsert_columns)
        )

    session.execute(statement)
    return len(records)
