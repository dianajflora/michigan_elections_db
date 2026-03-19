"""Create core election data tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260203_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the initial PostgreSQL schema."""

    op.create_table(
        "counties",
        sa.Column("county_id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("county_name", sa.String(length=255), nullable=False),
        sa.Column("fips_code", sa.String(length=16), nullable=False),
        sa.UniqueConstraint("county_name", "fips_code", name="uq_counties_name_fips"),
    )

    op.create_table(
        "elections",
        sa.Column("election_id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("election_year", sa.Integer(), nullable=False),
        sa.Column("election_date", sa.Date(), nullable=False),
        sa.Column("election_type", sa.String(length=100), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "election_year",
            "election_date",
            "election_type",
            name="uq_elections_year_date_type",
        ),
    )

    op.create_table(
        "jurisdictions",
        sa.Column("jurisdiction_id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("county_id", sa.Integer(), nullable=False),
        sa.Column("jurisdiction_name", sa.String(length=255), nullable=False),
        sa.Column("clerk_name", sa.String(length=255), nullable=True),
        sa.Column("clerk_email", sa.String(length=255), nullable=True),
        sa.Column("jurisdiction_type", sa.String(length=100), nullable=True),
        sa.ForeignKeyConstraint(
            ["county_id"],
            ["counties.county_id"],
            name="fk_jurisdictions_county_id_counties",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "county_id",
            "jurisdiction_name",
            name="uq_jurisdictions_county_name",
        ),
    )

    op.create_table(
        "locations",
        sa.Column("location_id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("jurisdiction_id", sa.Integer(), nullable=False),
        sa.Column("location_name", sa.String(length=255), nullable=False),
        sa.Column("address", sa.String(length=255), nullable=False),
        sa.Column("city", sa.String(length=255), nullable=True),
        sa.Column("zip_code", sa.String(length=20), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["jurisdiction_id"],
            ["jurisdictions.jurisdiction_id"],
            name="fk_locations_jurisdiction_id_jurisdictions",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "jurisdiction_id",
            "location_name",
            "address",
            name="uq_locations_jurisdiction_name_address",
        ),
    )

    op.create_table(
        "election_usage",
        sa.Column("usage_id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("election_id", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.Integer(), nullable=False),
        sa.Column("location_function", sa.String(length=100), nullable=False),
        sa.Column("open_date", sa.Date(), nullable=True),
        sa.Column("close_date", sa.Date(), nullable=True),
        sa.Column("hours", sa.String(length=100), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["election_id"],
            ["elections.election_id"],
            name="fk_election_usage_election_id_elections",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["locations.location_id"],
            name="fk_election_usage_location_id_locations",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "election_id",
            "location_id",
            "location_function",
            name="uq_election_usage_election_location_function",
        ),
    )

    op.create_index("ix_counties_county_name", "counties", ["county_name"], unique=False)
    op.create_index("ix_elections_election_date", "elections", ["election_date"], unique=False)
    op.create_index("ix_jurisdictions_county_id", "jurisdictions", ["county_id"], unique=False)
    op.create_index("ix_locations_jurisdiction_id", "locations", ["jurisdiction_id"], unique=False)
    op.create_index("ix_election_usage_election_id", "election_usage", ["election_id"], unique=False)
    op.create_index("ix_election_usage_location_id", "election_usage", ["location_id"], unique=False)


def downgrade() -> None:
    """Drop the initial PostgreSQL schema."""

    op.drop_index("ix_election_usage_location_id", table_name="election_usage")
    op.drop_index("ix_election_usage_election_id", table_name="election_usage")
    op.drop_index("ix_locations_jurisdiction_id", table_name="locations")
    op.drop_index("ix_jurisdictions_county_id", table_name="jurisdictions")
    op.drop_index("ix_elections_election_date", table_name="elections")
    op.drop_index("ix_counties_county_name", table_name="counties")
    op.drop_table("election_usage")
    op.drop_table("locations")
    op.drop_table("jurisdictions")
    op.drop_table("elections")
    op.drop_table("counties")
