"""Client-facing Streamlit app for safe PostgreSQL querying."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mielections.config.auth import login_gate
from mielections.config.settings import get_settings, mask_database_url
from mielections.db.session import session_scope
from mielections.query.service import (
    default_column_keys,
    execute_safe_query,
    get_column_options,
    get_filter_options,
    get_reachable_tables,
    get_table_counts,
    list_tables,
)


def _option_lookup(active_tables: list[str]) -> dict[str, object]:
    """Return query column options keyed by their stable ID."""

    return {option.key: option for option in get_column_options(active_tables)}


def _render_sidebar() -> None:
    """Render query app sidebar information."""

    settings = get_settings()
    st.sidebar.header("Connection")
    st.sidebar.code(mask_database_url(settings.database_url))
    st.sidebar.caption("Only predefined joins are allowed. Arbitrary SQL is not exposed.")

    with session_scope() as session:
        counts = get_table_counts(session)

    st.sidebar.header("Row Counts")
    for table_name in list_tables():
        st.sidebar.write(f"`{table_name}`: {counts[table_name]:,}")


def _render_filter_controls(base_table: str, active_tables: list[str], option_lookup: dict[str, object]) -> dict[str, list[object]]:
    """Render filter widgets and collect selected values."""

    filters: dict[str, list[object]] = {}
    filter_keys = st.multiselect(
        "Filter columns",
        options=list(option_lookup),
        format_func=lambda key: option_lookup[key].label,
        help="Filters are limited to distinct values from the selected safe tables.",
    )

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


def _render_results(base_table: str, selected_column_keys: list[str], filters: dict[str, list[object]], row_limit: int) -> None:
    """Run the safe query and render preview plus CSV export."""

    with session_scope() as session:
        frame = execute_safe_query(
            session=session,
            base_table=base_table,
            selected_column_keys=selected_column_keys,
            filters=filters,
            row_limit=row_limit,
        )

    st.subheader("Preview")
    st.caption(f"Returned {len(frame):,} row(s), preview capped at {row_limit:,}.")
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

    st.set_page_config(page_title="Michigan Elections Query App", layout="wide")
    st.title("Michigan Elections Query App")
    st.caption("Browse, filter, safely join related tables, and export CSV results.")

    if not login_gate(scope="query"):
        st.stop()

    _render_sidebar()

    base_table = st.selectbox("Base table", options=list_tables())
    reachable_tables = get_reachable_tables(base_table)
    default_related_tables = [table for table in reachable_tables if table != base_table]
    selected_related_tables = st.multiselect(
        "Related tables to include",
        options=[table for table in reachable_tables if table != base_table],
        default=default_related_tables,
        help="Only tables reachable through predefined safe joins can be selected.",
    )

    active_tables = [base_table, *selected_related_tables]
    option_lookup = _option_lookup(active_tables)

    st.subheader("Columns")
    selected_column_keys = st.multiselect(
        "Columns to display",
        options=list(option_lookup),
        default=[
            key for key in default_column_keys(base_table, active_tables) if key in option_lookup
        ],
        format_func=lambda key: option_lookup[key].label,
    )

    if not selected_column_keys:
        st.info("Select at least one column to preview results.")
        return

    st.subheader("Filters")
    filters = _render_filter_controls(base_table, active_tables, option_lookup)

    row_limit = st.slider("Preview row limit", min_value=25, max_value=1000, value=250, step=25)

    if st.button("Run Query", type="primary"):
        try:
            _render_results(base_table, selected_column_keys, filters, row_limit)
        except Exception as exc:
            st.error("Query execution failed.")
            st.exception(exc)
    else:
        st.info("Choose tables, columns, and optional filters, then run the query.")


if __name__ == "__main__":
    main()
