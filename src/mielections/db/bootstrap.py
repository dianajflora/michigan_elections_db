"""CLI helpers for creating or rebuilding the database schema."""

from __future__ import annotations

import argparse

from mielections.db.session import ensure_database_schema, rebuild_database_schema


def build_parser() -> argparse.ArgumentParser:
    """Return the CLI parser for schema bootstrap actions."""

    parser = argparse.ArgumentParser(description="Bootstrap the Michigan elections database schema.")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop and recreate the mutable schema objects for locations and election usage.",
    )
    parser.add_argument(
        "--drop-reference-data",
        action="store_true",
        help="Also drop and recreate counties and elections.",
    )
    return parser


def main() -> None:
    """Create or rebuild the configured database schema."""

    args = build_parser().parse_args()
    if args.rebuild:
        rebuild_database_schema(preserve_reference_tables=not args.drop_reference_data)
        return

    ensure_database_schema()


if __name__ == "__main__":
    main()
