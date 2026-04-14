"""Client-facing Streamlit app for safe PostgreSQL querying."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mielections.config.settings import get_settings
from mielections.db.session import ensure_database_schema, session_scope, set_database_url_key
from mielections.query.service import (
    default_column_keys,
    execute_safe_query,
    get_column_options,
    get_filter_options,
    get_reachable_tables,
    get_table_counts,
    list_tables,
)

BASE_TABLE_JOIN_OPTIONS: dict[str, tuple[str, ...]] = {
    "counties": (),
    "locations": ("counties",),
    "election_usage": ("locations", "counties", "elections"),
    "elections": (),
}
AUTO_INCLUDED_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("locations", "counties"): ("counties.county_name",),
}


def _option_lookup(active_tables: list[str]) -> dict[str, object]:
    """Return query column options keyed by their stable ID."""

    return {
        option.key: option
        for option in get_column_options(active_tables)
        if not option.column_name.endswith("_id")
    }


def _auto_included_column_keys(base_table: str, selected_related_tables: list[str]) -> list[str]:
    """Return columns that should be auto-added for specific join choices."""

    column_keys: list[str] = []
    for related_table in selected_related_tables:
        column_keys.extend(AUTO_INCLUDED_COLUMNS.get((base_table, related_table), ()))
    return column_keys


def _render_sidebar() -> None:
    """Render query app sidebar information."""

    get_settings()

    with session_scope() as session:
        counts = get_table_counts(session)

    st.sidebar.header("Row Counts")
    for table_name in list_tables():
        st.sidebar.write(f"`{table_name}`: {counts[table_name]:,}")


def _render_filter_controls(base_table: str, active_tables: list[str], option_lookup: dict[str, object]) -> dict[str, list[object]]:
    """Render filter widgets and collect selected values."""

    filters: dict[str, list[object]] = {}
    filter_widget_key = f"filter_columns_{base_table}_{'_'.join(active_tables)}"
    filter_keys = st.multiselect(
        "Filter columns",
        options=list(option_lookup),
        format_func=lambda key: option_lookup[key].label,
        help="Filters are limited to distinct values from the selected safe tables.",
        key=filter_widget_key,
    )
    filter_keys = [key for key in filter_keys if key in option_lookup]

    if not filter_keys:
        return filters

    with session_scope() as session:
        for key in filter_keys:
            option = option_lookup[key]
            values = get_filter_options(session, base_table, key, active_tables)
            filters[key] = st.multiselect(
                f"Values for {option.label}",
                options=values,
                key=f"filter_{key}",
            )

    return filters


def _render_results(base_table: str, selected_column_keys: list[str], filters: dict[str, list[object]]) -> None:
    """Run the safe query and render preview plus CSV export."""

    with session_scope() as session:
        frame = execute_safe_query(
            session=session,
            base_table=base_table,
            selected_column_keys=selected_column_keys,
            filters=filters,
            row_limit=None,
        )

    st.subheader("Preview")
    st.caption(f"Returned {len(frame):,} row(s).")
    st.dataframe(frame, use_container_width=True, hide_index=True)

    csv_bytes = frame.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download CSV",
        data=csv_bytes,
        file_name=f"{base_table}_query_results.csv",
        mime="text/csv",
        disabled=frame.empty,
    )


def main() -> None:
    """Render the client-facing safe query app."""

    set_database_url_key("QUERY_DATABASE_URL")
    st.set_page_config(page_title="Michigan Elections Query App", layout="wide")
    ensure_database_schema()
    st.title("Michigan Elections Query App")
    st.caption("Browse, filter, safely join related tables, and export CSV results.")

    _render_sidebar()

    base_table = st.selectbox("Base table", options=list_tables())
    available_related_tables = list(BASE_TABLE_JOIN_OPTIONS.get(base_table, ()))

    if base_table == "locations":
        include_counties = st.toggle("Include counties", value=True)
        selected_related_tables = ["counties"] if include_counties else []
    elif available_related_tables:
        selected_related_tables = st.multiselect(
            "Related tables to include",
            options=available_related_tables,
            default=available_related_tables,
            help="Only base-table-specific safe joins are available.",
        )
    else:
        st.caption("No joins are available for this base table.")
        selected_related_tables = []

    active_tables = [base_table, *selected_related_tables]
    option_lookup = _option_lookup(active_tables)
    auto_column_keys = [key for key in _auto_included_column_keys(base_table, selected_related_tables) if key in option_lookup]

    st.subheader("Columns")
    column_widget_key = f"columns_{base_table}_{'_'.join(active_tables)}"
    selected_column_keys = st.multiselect(
        "Columns to display",
        options=list(option_lookup),
        default=[
            key for key in default_column_keys(base_table, active_tables) if key in option_lookup
        ] + [key for key in auto_column_keys if key not in default_column_keys(base_table, active_tables)],
        format_func=lambda key: option_lookup[key].label,
        key=column_widget_key,
    )
    selected_column_keys = [key for key in selected_column_keys if key in option_lookup]
    selected_column_keys = [*selected_column_keys, *[key for key in auto_column_keys if key not in selected_column_keys]]

    if not selected_column_keys:
        st.info("Select at least one column to preview results.")
        return

    st.subheader("Filters")
    filters = _render_filter_controls(base_table, active_tables, option_lookup)

    if st.button("Run Query", type="primary"):
        try:
            _render_results(base_table, selected_column_keys, filters)
        except Exception as exc:
            st.error("Query execution failed.")
            st.exception(exc)
    else:
        st.info("Choose tables, columns, and optional filters, then run the query.")


if __name__ == "__main__":
    main()
