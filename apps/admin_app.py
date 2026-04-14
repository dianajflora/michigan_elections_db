"""Streamlit admin app for validated CSV uploads into PostgreSQL."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mielections.config.settings import get_settings
from mielections.config.table_metadata import TABLE_DEFINITIONS, ordered_table_definitions
from mielections.db.session import ensure_database_schema, set_database_url_key
from mielections.etl.exceptions import EtlValidationError
from mielections.etl.service import execute_upload, preview_upload
from mielections.etl.validation import ValidationIssue

ADMIN_UPLOAD_TABLES = {
    definition.table_name: definition
    for definition in ordered_table_definitions()
    if definition.table_name != "counties"
}


def issues_to_frame(issues: list[ValidationIssue]) -> pd.DataFrame:
    """Convert validation issues into a display-friendly DataFrame."""

    return pd.DataFrame(
        [
            {
                "row_number": issue.row_number,
                "column_name": issue.column_name,
                "message": issue.message,
            }
            for issue in issues
        ]
    )


def render_table_expectations(table_name: str) -> None:
    """Render the expected CSV schema for the selected table."""

    table_definition = TABLE_DEFINITIONS[table_name]
    expectation_rows = [
        {
            "csv_column": column.source_name,
            "target_field": column.target_name,
            "type": column.dtype,
            "required": column.required,
            "description": column.description,
        }
        for column in table_definition.columns
    ]
    st.dataframe(pd.DataFrame(expectation_rows), use_container_width=True, hide_index=True)


def render_sidebar() -> None:
    """Render shared sidebar content."""

    get_settings()

    st.sidebar.header("Load Order")
    for table_definition in ADMIN_UPLOAD_TABLES.values():
        st.sidebar.write(f"{table_definition.load_order}. `{table_definition.table_name}`")


def main() -> None:
    """Render the admin uploader UI."""

    set_database_url_key("ADMIN_DATABASE_URL")
    st.set_page_config(page_title="Michigan Elections Admin Upload", layout="wide")
    ensure_database_schema()
    st.title("Michigan Elections Admin Uploader")
    st.caption("Validated CSV uploads into the shared PostgreSQL database.")

    render_sidebar()

    table_names = list(ADMIN_UPLOAD_TABLES)
    selected_table = st.selectbox("Target table", options=table_names)

    st.subheader("Expected CSV columns")
    render_table_expectations(selected_table)

    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

    if uploaded_file is None:
        st.info("Choose a CSV file to validate and preview before upload.")
        return

    file_bytes = uploaded_file.getvalue()

    try:
        preview = preview_upload(selected_table, file_bytes)
    except Exception as exc:
        st.error("The file could not be validated.")
        st.exception(exc)
        return

    st.subheader("Preview")
    st.dataframe(preview.preview_frame, use_container_width=True, hide_index=True)

    if preview.warnings:
        st.subheader("Warnings")
        st.dataframe(issues_to_frame(preview.warnings), use_container_width=True, hide_index=True)

    if preview.errors:
        st.subheader("Validation errors")
        st.dataframe(issues_to_frame(preview.errors), use_container_width=True, hide_index=True)
        st.stop()

    st.success(f"Validation passed for {len(preview.normalized_frame)} row(s).")

    if st.button("Upload To Database", type="primary"):
        try:
            result = execute_upload(selected_table, file_bytes)
        except EtlValidationError as exc:
            st.error("Upload blocked by validation errors.")
            st.dataframe(issues_to_frame(exc.issues), use_container_width=True, hide_index=True)
            return
        except Exception as exc:
            st.error("Upload failed. The transaction was rolled back.")
            st.exception(exc)
            return

        st.success(
            f"Upload committed successfully. Upserted {result.row_count} row(s) into `{result.table_name}`."
        )


if __name__ == "__main__":
    main()
