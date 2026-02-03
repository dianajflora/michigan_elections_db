import streamlit as st
import sqlite3
import pandas as pd
from typing import List, Dict, Any

# Path to your SQLite database
DB_PATH = "polling.db"

# Tables the user can explore
AVAILABLE_TABLES = [
    "counties",
    "jurisdictions",
    "elections",
    "locations",
    "election_usage"
]

# For FK dropdowns: which column should be displayed for each referenced table
REF_TABLE_DISPLAY_COL = {
    "counties": "county_name",
    "jurisdictions": "jurisdiction_name",
    "locations": "location_name",
    "elections": "election_date"
}

# ----------------------------
# SCHEMA INSPECTION
# ----------------------------
def get_table_schema(table_name: str) -> list[dict]:
    """Returns column definitions including PK, FK, and referenced tables."""
    with sqlite3.connect(DB_PATH) as conn:
        table_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        fk_info = conn.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()

    # Build FK mapping: column → (ref_table, ref_column)
    fk_map = {}
    for row in fk_info:
        _, _, ref_table, from_col, to_col, *_ = row
        fk_map[from_col] = {"ref_table": ref_table, "ref_column": to_col}

    columns = []
    for cid, name, col_type, notnull, default_val, pk in table_info:
        is_fk = name in fk_map

        display_name = name[:-3] if (is_fk and name.endswith("_id")) else name

        columns.append(
            {
                "name": name,
                "display_name": display_name,
                "type": col_type.upper() if col_type else "",
                "notnull": notnull,
                "pk": pk,
                "fk": int(is_fk),
                "ref_table": fk_map[name]["ref_table"] if is_fk else None,
                "ref_column": fk_map[name]["ref_column"] if is_fk else None,
            }
        )
    return columns


# ----------------------------
# OPTION VALUE FETCHING
# ----------------------------
def get_distinct_values(table_name: str, col: dict):
    """Returns dropdown values for this column."""
    name = col["name"]

    # If foreign key → pull display values from referenced table
    if col["fk"]:
        ref_table = col["ref_table"]
        display_col = REF_TABLE_DISPLAY_COL.get(ref_table)

        # If no display mapping defined, fallback to FK id
        if display_col is None:
            display_col = col["ref_column"]

        query = f"SELECT DISTINCT {display_col} FROM {ref_table} ORDER BY {display_col}"
        with sqlite3.connect(DB_PATH) as conn:
            return pd.read_sql(query, conn)[display_col].dropna().tolist()

    # Otherwise → use column's own distinct values
    query = f"SELECT DISTINCT {name} FROM {table_name} ORDER BY {name}"
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql(query, conn)[name].dropna().tolist()


# ----------------------------
# QUERY EXECUTION
# ----------------------------
def run_filtered_query(table_name: str, columns: list[dict], filters: Dict[str, List[Any]]) -> pd.DataFrame:
    """
    Executes a relational query that automatically joins foreign key tables
    when filters target FK display values. Returns DataFrame for CSV export.
    """

    base_alias = table_name  # helpful if we later add multiple joins

    # SELECT base table columns
    select_cols = [f"{base_alias}.{col['name']}" for col in columns]

    joins = []
    where_clauses = []
    parameters: List[Any] = []

    # Build joins + SELECT extensions for FK columns
    for col in columns:
        if col["fk"]:
            ref_table = col["ref_table"]
            ref_id_col = col["ref_column"]
            display_col = REF_TABLE_DISPLAY_COL.get(ref_table, ref_id_col)

            # JOIN clause
            joins.append(
                f"JOIN {ref_table} ON {base_alias}.{col['name']} = {ref_table}.{ref_id_col}"
            )

            # include display value in result
            select_cols.append(f"{ref_table}.{display_col} AS {col['display_name']}")

    # Build WHERE clauses using filters
    for col in columns:
        col_name = col["name"]
        vals = filters.get(col_name, [])

        if not vals:
            continue

        if col["fk"]:
            # filter on referenced display column (ex: counties.county_name)
            ref_table = col["ref_table"]
            display_col = REF_TABLE_DISPLAY_COL.get(ref_table, col["ref_column"])
            placeholders = ", ".join(["?"] * len(vals))
            where_clauses.append(f"{ref_table}.{display_col} IN ({placeholders})")
            parameters.extend(vals)
        else:
            # normal filter on base column
            placeholders = ", ".join(["?"] * len(vals))
            where_clauses.append(f"{base_alias}.{col_name} IN ({placeholders})")
            parameters.extend(vals)

    # Build final SQL
    query = f"SELECT {', '.join(select_cols)} FROM {base_alias} "

    if joins:
        query += " " + " ".join(joins)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=parameters)

    df_columns = df.columns

    return df[[col for col in df_columns if not col.endswith("_id")]]

# ----------------------------
# STREAMLIT UI
# ----------------------------
def main():
    st.title("Election Data Explorer")
    st.sidebar.header("Database Settings")
    st.sidebar.write(f"Using database: `{DB_PATH}`")

    table_name = st.selectbox("Select a table to explore", AVAILABLE_TABLES)

    if not table_name:
        st.stop()

    st.subheader(f"Filters for `{table_name}`")

    columns = get_table_schema(table_name)
    filters: Dict[str, List[Any]] = {}

    for col in columns:
        # Skip primary key columns from filtering
        if col["pk"] == 1:
            continue

        ui_label = col["display_name"]     # label for UI
        sql_column = col["name"]          # actual DB column name

        try:
            distinct_values = get_distinct_values(table_name, col)
        except Exception:
            continue

        if not distinct_values:
            continue

        selected = st.multiselect(
            label=f"Filter by `{ui_label}`",
            options=distinct_values,
            default=[],
            key=f"{table_name}_{sql_column}",
        )

        filters[sql_column] = selected

    if st.button("Run Query"):
        df = run_filtered_query(table_name, columns, filters)
        st.write(f"Returned {len(df)} rows.")
        st.dataframe(df)

        if not df.empty:
            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download filtered data as CSV",
                data=csv_data,
                file_name=f"{table_name}_filtered.csv",
                mime="text/csv"
            )
    else:
        st.info("Set filters (optional) and click **Run Query** to see results.")

if __name__ == "__main__":
    main()