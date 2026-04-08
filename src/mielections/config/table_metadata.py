"""Column metadata used by ETL and the future query UI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CsvColumnDefinition:
    """Define a supported CSV column and how it maps into the application."""

    source_name: str
    target_name: str
    dtype: str
    required: bool
    description: str = ""


@dataclass(frozen=True)
class TableDefinition:
    """Describe ETL expectations and upsert behavior for a table."""

    table_name: str
    display_name: str
    load_order: int
    columns: tuple[CsvColumnDefinition, ...]
    upsert_columns: tuple[str, ...]
    preview_columns: tuple[str, ...]


TABLE_DEFINITIONS: dict[str, TableDefinition] = {
    "counties": TableDefinition(
        table_name="counties",
        display_name="Counties",
        load_order=1,
        columns=(
            CsvColumnDefinition("FIPS", "fips_code", "string", True, "County FIPS code."),
            CsvColumnDefinition("County_Name", "county_name", "string", True, "County display name."),
        ),
        upsert_columns=("county_name", "fips_code"),
        preview_columns=("county_name", "fips_code"),
    ),
    "locations": TableDefinition(
        table_name="locations",
        display_name="Locations",
        load_order=2,
        columns=(
            CsvColumnDefinition("county_name", "county_name", "string", True, "Existing county name."),
            CsvColumnDefinition("location_name", "location_name", "string", True, "Location name."),
            CsvColumnDefinition("address", "address", "string", True, "Street address."),
            CsvColumnDefinition("city", "city", "string", False, "City."),
            CsvColumnDefinition("zip_code", "zip_code", "string", False, "ZIP code."),
            CsvColumnDefinition("jurisdiction_name", "jurisdiction_name", "string", False, "Jurisdiction name for this location."),
            CsvColumnDefinition("Precinct", "precinct", "string", False, "Precinct served by this location."),
            CsvColumnDefinition("latitude", "latitude", "float", False, "Latitude in decimal degrees."),
            CsvColumnDefinition("longitude", "longitude", "float", False, "Longitude in decimal degrees."),
            CsvColumnDefinition("handicap_accessible", "handicap_accessible", "boolean", False, "Whether the location is handicap accessible."),
            CsvColumnDefinition("access_notes", "access_notes", "string", False, "Accessibility notes."),
            CsvColumnDefinition("location_description", "location_description", "string", False, "Additional location description."),
        ),
        upsert_columns=("county_id", "location_name", "address"),
        preview_columns=(
            "county_name",
            "jurisdiction_name",
            "location_name",
            "address",
            "city",
            "precinct",
            "handicap_accessible",
        ),
    ),
    "elections": TableDefinition(
        table_name="elections",
        display_name="Elections",
        load_order=3,
        columns=(
            CsvColumnDefinition("election_year", "election_year", "integer", True, "Election year."),
            CsvColumnDefinition("election_date", "election_date", "date", True, "Election date."),
            CsvColumnDefinition("election_type", "election_type", "string", True, "Election type."),
            CsvColumnDefinition("notes", "notes", "string", False, "Optional notes."),
        ),
        upsert_columns=("election_year", "election_date", "election_type"),
        preview_columns=("election_year", "election_date", "election_type", "notes"),
    ),
    "election_usage": TableDefinition(
        table_name="election_usage",
        display_name="Election Usage",
        load_order=4,
        columns=(
            CsvColumnDefinition("county_name", "county_name", "string", True, "Existing county name."),
            CsvColumnDefinition("location_name", "location_name", "string", True, "Existing location name."),
            CsvColumnDefinition("address", "address", "string", True, "Existing location address."),
            CsvColumnDefinition("election_type", "election_type", "string", True, "Existing election type."),
            CsvColumnDefinition("election_date", "election_date", "date", True, "Existing election date."),
            CsvColumnDefinition("location_function", "location_function", "string", True, "How the location is used."),
            CsvColumnDefinition("day", "day", "date", True, "Day or date label for the usage window."),
            CsvColumnDefinition("hour", "hour", "string", True, "Hour or hour range."),
        ),
        upsert_columns=("election_id", "location_id", "location_function", "day", "hour"),
        preview_columns=(
            "county_name",
            "location_name",
            "election_type",
            "election_date",
            "location_function",
            "day",
            "hour",
        ),
    ),
}


def ordered_table_definitions() -> list[TableDefinition]:
    """Return tables in their required load order."""

    return sorted(TABLE_DEFINITIONS.values(), key=lambda item: item.load_order)
