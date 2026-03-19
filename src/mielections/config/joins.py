"""Allowed join metadata for the future safe query UI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AllowedJoin:
    """A predefined relationship that the query app may expose."""

    left_table: str
    right_table: str
    left_column: str
    right_column: str
    relationship_name: str


ALLOWED_JOINS: tuple[AllowedJoin, ...] = (
    AllowedJoin("counties", "jurisdictions", "county_id", "county_id", "county_to_jurisdictions"),
    AllowedJoin("jurisdictions", "locations", "jurisdiction_id", "jurisdiction_id", "jurisdiction_to_locations"),
    AllowedJoin("elections", "election_usage", "election_id", "election_id", "election_to_usage"),
    AllowedJoin("locations", "election_usage", "location_id", "location_id", "location_to_usage"),
)
