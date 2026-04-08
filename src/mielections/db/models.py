"""SQLAlchemy models for the election data platform."""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Boolean, Date, Float, ForeignKey, Identity, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mielections.db.base import Base


class County(Base):
    """County reference data."""

    __tablename__ = "counties"
    __table_args__ = (UniqueConstraint("county_name", "fips_code", name="uq_counties_name_fips"),)

    county_id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    county_name: Mapped[str] = mapped_column(String(255), nullable=False)
    fips_code: Mapped[str] = mapped_column(String(16), nullable=False)

    locations: Mapped[list["Location"]] = relationship(back_populates="county")


class Location(Base):
    """Physical election-related location."""

    __tablename__ = "locations"
    __table_args__ = (
        UniqueConstraint(
            "county_id",
            "location_name",
            "address",
            name="uq_locations_county_name_address",
        ),
    )

    location_id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    county_id: Mapped[int] = mapped_column(
        ForeignKey("counties.county_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    location_name: Mapped[str] = mapped_column(String(255), nullable=False)
    address: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    zip_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    jurisdiction_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    precinct: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    handicap_accessible: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    access_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    county: Mapped[County] = relationship(back_populates="locations")
    election_usage: Mapped[list["ElectionUsage"]] = relationship(back_populates="location")


class Election(Base):
    """Election reference data."""

    __tablename__ = "elections"
    __table_args__ = (
        UniqueConstraint(
            "election_year",
            "election_date",
            "election_type",
            name="uq_elections_year_date_type",
        ),
    )

    election_id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    election_year: Mapped[int] = mapped_column(Integer, nullable=False)
    election_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    election_type: Mapped[str] = mapped_column(String(100), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    election_usage: Mapped[list["ElectionUsage"]] = relationship(back_populates="election")


class ElectionUsage(Base):
    """How a location is used in a specific election."""

    __tablename__ = "election_usage"
    __table_args__ = (
        UniqueConstraint(
            "election_id",
            "location_id",
            "location_function",
            "day",
            "hour",
            name="uq_election_usage_election_location_schedule",
        ),
    )

    usage_id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    election_id: Mapped[int] = mapped_column(
        ForeignKey("elections.election_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    location_id: Mapped[int] = mapped_column(
        ForeignKey("locations.location_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    location_function: Mapped[str] = mapped_column(String(100), nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    hour: Mapped[str] = mapped_column(String(100), nullable=False)

    election: Mapped[Election] = relationship(back_populates="election_usage")
    location: Mapped[Location] = relationship(back_populates="election_usage")
