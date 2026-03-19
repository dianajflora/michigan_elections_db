"""Safe query builder and metadata helpers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
from sqlalchemy import Select, func, inspect, select
from sqlalchemy.orm import DeclarativeBase, Session

from mielections.config.joins import ALLOWED_JOINS, AllowedJoin
from mielections.db.models import County, Election, ElectionUsage, Jurisdiction, Location

MODEL_REGISTRY: dict[str, type[DeclarativeBase]] = {
    "counties": County,
    "jurisdictions": Jurisdiction,
    "locations": Location,
    "elections": Election,
    "election_usage": ElectionUsage,
}

DISPLAY_NAME_OVERRIDES: dict[str, str] = {
    "counties.county_id": "County ID",
    "counties.county_name": "County Name",
    "counties.fips_code": "FIPS Code",
    "jurisdictions.jurisdiction_id": "Jurisdiction ID",
    "jurisdictions.county_id": "County ID",
    "jurisdictions.jurisdiction_name": "Jurisdiction Name",
    "jurisdictions.clerk_name": "Clerk Name",
    "jurisdictions.clerk_email": "Clerk Email",
    "jurisdictions.jurisdiction_type": "Jurisdiction Type",
    "locations.location_id": "Location ID",
    "locations.jurisdiction_id": "Jurisdiction ID",
    "locations.location_name": "Location Name",
    "locations.address": "Address",
    "locations.city": "City",
    "locations.zip_code": "ZIP Code",
    "locations.latitude": "Latitude",
    "locations.longitude": "Longitude",
    "elections.election_id": "Election ID",
    "elections.election_year": "Election Year",
    "elections.election_date": "Election Date",
    "elections.election_type": "Election Type",
    "elections.notes": "Election Notes",
    "election_usage.usage_id": "Usage ID",
    "election_usage.election_id": "Election ID",
    "election_usage.location_id": "Location ID",
    "election_usage.location_function": "Location Function",
    "election_usage.open_date": "Open Date",
    "election_usage.close_date": "Close Date",
    "election_usage.hours": "Hours",
    "election_usage.notes": "Usage Notes",
}


@dataclass(frozen=True)
class ColumnOption:
    """A selectable column in the query UI."""

    key: str
    table_name: str
    column_name: str
    label: str
    python_type: str


def _join_graph() -> dict[str, list[tuple[str, AllowedJoin, bool]]]:
    """Build an undirected graph of allowed joins."""

    graph: dict[str, list[tuple[str, AllowedJoin, bool]]] = {}
    for join in ALLOWED_JOINS:
        graph.setdefault(join.left_table, []).append((join.right_table, join, True))
        graph.setdefault(join.right_table, []).append((join.left_table, join, False))
    return graph


def list_tables() -> list[str]:
    """Return known table names in a stable order."""

    return list(MODEL_REGISTRY)


def get_table_counts(session: Session) -> dict[str, int]:
    """Return row counts for the browsable tables."""

    counts: dict[str, int] = {}
    for table_name, model in MODEL_REGISTRY.items():
        counts[table_name] = session.scalar(select(func.count()).select_from(model)) or 0
    return counts


def get_reachable_tables(base_table: str) -> list[str]:
    """Return tables reachable from the selected base table via safe joins."""

    graph = _join_graph()
    visited: set[str] = {base_table}
    queue: deque[str] = deque([base_table])

    while queue:
        current = queue.popleft()
        for neighbor, _, _ in graph.get(current, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

    return [table for table in list_tables() if table in visited]


def find_join_path(base_table: str, target_table: str) -> list[tuple[AllowedJoin, bool]]:
    """Return the safe join path from base_table to target_table."""

    if base_table == target_table:
        return []

    graph = _join_graph()
    visited: set[str] = {base_table}
    queue: deque[tuple[str, list[tuple[AllowedJoin, bool]]]] = deque([(base_table, [])])

    while queue:
        current, path = queue.popleft()
        for neighbor, join, forward in graph.get(current, []):
            if neighbor in visited:
                continue
            next_path = [*path, (join, forward)]
            if neighbor == target_table:
                return next_path
            visited.add(neighbor)
            queue.append((neighbor, next_path))

    raise ValueError(f"No allowed join path from {base_table} to {target_table}.")


def get_column_options(table_names: list[str]) -> list[ColumnOption]:
    """Return browsable columns for the given tables."""

    options: list[ColumnOption] = []
    for table_name in table_names:
        model = MODEL_REGISTRY[table_name]
        mapper = inspect(model)
        for column in mapper.columns:
            key = f"{table_name}.{column.key}"
            try:
                python_type = column.type.python_type.__name__
            except NotImplementedError:
                python_type = "str"
            options.append(
                ColumnOption(
                    key=key,
                    table_name=table_name,
                    column_name=column.key,
                    label=DISPLAY_NAME_OVERRIDES.get(key, f"{table_name}.{column.key}"),
                    python_type=python_type,
                )
            )
    return options


def default_column_keys(base_table: str, active_tables: list[str]) -> list[str]:
    """Return a sensible default column selection."""

    all_options = {option.key: option for option in get_column_options(active_tables)}
    preferred_keys = [
        key
        for key in DISPLAY_NAME_OVERRIDES
        if key in all_options and not key.endswith("_id")
    ]
    base_defaults = [key for key in preferred_keys if key.startswith(f"{base_table}.")]
    return base_defaults[:6] or list(all_options)[:6]


def _apply_required_joins(statement: Select, base_table: str, tables_to_include: set[str]) -> Select:
    """Add all joins needed to reach the requested tables."""

    joined_tables: set[str] = {base_table}
    for table_name in tables_to_include:
        if table_name == base_table:
            continue
        for join, forward in find_join_path(base_table, table_name):
            left_table = join.left_table if forward else join.right_table
            right_table = join.right_table if forward else join.left_table
            if right_table in joined_tables:
                continue
            left_model = MODEL_REGISTRY[left_table]
            right_model = MODEL_REGISTRY[right_table]
            left_column = getattr(left_model, join.left_column if forward else join.right_column)
            right_column = getattr(right_model, join.right_column if forward else join.left_column)
            statement = statement.join(right_model, left_column == right_column)
            joined_tables.add(right_table)
    return statement


def _coerce_filter_values(values: list[object]) -> list[object]:
    """Normalize values before applying filters or exporting."""

    coerced: list[object] = []
    for value in values:
        if isinstance(value, pd.Timestamp):
            coerced.append(value.date())
        else:
            coerced.append(value)
    return coerced


def get_filter_options(
    session: Session,
    base_table: str,
    target_column_key: str,
    active_tables: list[str],
    limit: int = 200,
) -> list[object]:
    """Return distinct filter values for a safe column selection."""

    table_name, column_name = target_column_key.split(".", maxsplit=1)
    model = MODEL_REGISTRY[base_table]
    target_model = MODEL_REGISTRY[table_name]
    target_column = getattr(target_model, column_name)

    statement = select(target_column).select_from(model).distinct()
    statement = _apply_required_joins(statement, base_table, set(active_tables) | {table_name})
    statement = statement.where(target_column.is_not(None)).order_by(target_column).limit(limit)

    values = session.scalars(statement).all()
    return [value for value in values if value is not None]


def execute_safe_query(
    session: Session,
    base_table: str,
    selected_column_keys: list[str],
    filters: dict[str, list[object]],
    row_limit: int,
) -> pd.DataFrame:
    """Build and execute a safe query constrained by allowed joins."""

    if not selected_column_keys:
        return pd.DataFrame()

    requested_tables = {key.split(".", maxsplit=1)[0] for key in selected_column_keys}
    requested_tables.update(key.split(".", maxsplit=1)[0] for key, values in filters.items() if values)

    selected_columns = []
    for key in selected_column_keys:
        table_name, column_name = key.split(".", maxsplit=1)
        model = MODEL_REGISTRY[table_name]
        label = DISPLAY_NAME_OVERRIDES.get(key, key)
        selected_columns.append(getattr(model, column_name).label(label))

    base_model = MODEL_REGISTRY[base_table]
    statement = select(*selected_columns).select_from(base_model)
    statement = _apply_required_joins(statement, base_table, requested_tables)

    for key, values in filters.items():
        if not values:
            continue
        table_name, column_name = key.split(".", maxsplit=1)
        model = MODEL_REGISTRY[table_name]
        statement = statement.where(getattr(model, column_name).in_(_coerce_filter_values(values)))

    statement = statement.limit(row_limit)
    result = session.execute(statement)
    frame = pd.DataFrame(result.fetchall(), columns=result.keys())
    return frame.applymap(_serialize_value) if not frame.empty else frame


def _serialize_value(value: object) -> object:
    """Normalize SQLAlchemy result values for Streamlit and CSV export."""

    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value
