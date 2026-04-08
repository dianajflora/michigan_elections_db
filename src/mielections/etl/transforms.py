"""CSV normalization and foreign key resolution helpers."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from mielections.db.models import County, Election, Location
from mielections.etl.validation import ValidationIssue


def _normalize_string(value: object) -> str | None:
    """Strip strings and convert blank values to None."""

    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    return text or None


def prepare_counties(df: pd.DataFrame, session: Session) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Return county rows ready for loading."""

    del session
    return df[["county_name", "fips_code"]].copy(), []


def prepare_locations(
    df: pd.DataFrame,
    session: Session,
) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Resolve county IDs for location rows."""

    counties = session.execute(select(County.county_id, County.county_name)).all()
    county_lookup = {
        _normalize_string(county_name): county_id
        for county_id, county_name in counties
    }

    issues: list[ValidationIssue] = []
    county_ids: list[int | None] = []

    for index, row in df.iterrows():
        county_name = _normalize_string(row["county_name"])
        county_id = county_lookup.get(county_name)
        if county_id is None:
            issues.append(
                ValidationIssue(
                    row_number=index + 2,
                    column_name="county_name",
                    message=f"County '{county_name}' does not exist in PostgreSQL.",
                )
            )
        county_ids.append(county_id)

    prepared = df.copy()
    prepared["county_id"] = county_ids

    return prepared[
        [
            "county_id",
            "location_name",
            "address",
            "city",
            "zip_code",
            "jurisdiction_name",
            "precinct",
            "latitude",
            "longitude",
            "handicap_accessible",
            "access_notes",
            "location_description",
        ]
    ], issues


def prepare_elections(df: pd.DataFrame, session: Session) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Validate election-specific rules and return load-ready rows."""

    del session
    issues: list[ValidationIssue] = []

    for index, row in df.iterrows():
        election_date = row["election_date"]
        election_year = row["election_year"]
        if isinstance(election_date, date) and election_year is not None and election_date.year != election_year:
            issues.append(
                ValidationIssue(
                    row_number=index + 2,
                    column_name="election_year",
                    message="Election year does not match the year in election_date.",
                )
            )

    return df[["election_year", "election_date", "election_type", "notes"]].copy(), issues


def prepare_election_usage(
    df: pd.DataFrame,
    session: Session,
) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Resolve election and location IDs for election usage rows."""

    election_rows = session.execute(
        select(Election.election_id, Election.election_date, Election.election_type)
    ).all()
    election_lookup = {
        (election_date, _normalize_string(election_type)): election_id
        for election_id, election_date, election_type in election_rows
    }

    location_rows = session.execute(
        select(Location.location_id, Location.location_name, Location.address, County.county_name).join(
            County,
            County.county_id == Location.county_id,
        )
    ).all()
    location_lookup: dict[tuple[str | None, str | None, str | None], list[int]] = {}
    for location_id, location_name, address, county_name in location_rows:
        key = (
            _normalize_string(county_name),
            _normalize_string(location_name),
            _normalize_string(address),
        )
        location_lookup.setdefault(key, []).append(location_id)

    issues: list[ValidationIssue] = []
    election_ids: list[int | None] = []
    location_ids: list[int | None] = []

    for index, row in df.iterrows():
        election_key = (row["election_date"], _normalize_string(row["election_type"]))
        election_id = election_lookup.get(election_key)
        if election_id is None:
            issues.append(
                ValidationIssue(
                    row_number=index + 2,
                    column_name="election_date",
                    message=(
                        f"Election '{row['election_type']}' on '{row['election_date']}' "
                        "does not exist in PostgreSQL."
                    ),
                )
            )

        county_name = _normalize_string(row["county_name"])
        location_key = (
            county_name,
            _normalize_string(row["location_name"]),
            _normalize_string(row["address"]),
        )
        matching_location_ids = location_lookup.get(location_key, [])
        if not matching_location_ids:
            issues.append(
                ValidationIssue(
                    row_number=index + 2,
                    column_name="location_name",
                    message=(
                        f"Location '{row['location_name']}' at '{row['address']}' in county "
                        f"'{row['county_name']}' does not exist in PostgreSQL."
                    ),
                )
            )
            location_id = None
        elif len(matching_location_ids) > 1:
            issues.append(
                ValidationIssue(
                    row_number=index + 2,
                    column_name="location_name",
                    message=(
                        f"Location '{row['location_name']}' at '{row['address']}' in county "
                        f"'{row['county_name']}' is ambiguous."
                    ),
                )
            )
            location_id = None
        else:
            location_id = matching_location_ids[0]

        election_ids.append(election_id)
        location_ids.append(location_id)

    prepared = df.copy()
    prepared["election_id"] = election_ids
    prepared["location_id"] = location_ids

    return prepared[
        [
            "election_id",
            "location_id",
            "location_function",
            "day",
            "hour",
        ]
    ], issues


PREPARERS = {
    "counties": prepare_counties,
    "locations": prepare_locations,
    "elections": prepare_elections,
    "election_usage": prepare_election_usage,
}


def prepare_for_load(
    table_name: str,
    df: pd.DataFrame,
    session: Session,
) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Resolve table-specific lookups and produce model-ready rows."""

    try:
        preparer = PREPARERS[table_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported table: {table_name}") from exc

    return preparer(df, session)
