"""CSV parsing and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO

import pandas as pd
from sqlalchemy.orm import Session

from mielections.config.table_metadata import TABLE_DEFINITIONS, TableDefinition


@dataclass(frozen=True)
class ValidationIssue:
    """A validation warning or error."""

    message: str
    row_number: int | None = None
    column_name: str | None = None


@dataclass
class ValidationResult:
    """The result of validating and preparing an upload."""

    table_name: str
    normalized_frame: pd.DataFrame
    preview_frame: pd.DataFrame
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Return True when no validation errors were collected."""

        return not self.errors


def normalize_column_name(column_name: str) -> str:
    """Normalize source CSV column names for matching."""

    return column_name.strip().lower().replace(" ", "_")


def read_csv_bytes(file_bytes: bytes) -> pd.DataFrame:
    """Read uploaded CSV bytes into a pandas DataFrame."""

    return pd.read_csv(BytesIO(file_bytes))


def align_columns(df: pd.DataFrame, table_definition: TableDefinition) -> tuple[pd.DataFrame, list[ValidationIssue], list[ValidationIssue]]:
    """Rename supported columns and report missing or unexpected ones."""

    normalized_columns = {column: normalize_column_name(column) for column in df.columns}
    df = df.rename(columns=normalized_columns)

    source_to_target = {
        normalize_column_name(column.source_name): column.target_name
        for column in table_definition.columns
    }
    expected_source_names = set(source_to_target)
    incoming_source_names = set(df.columns)

    warnings = [
        ValidationIssue(message=f"Unexpected column '{column_name}' will be ignored.")
        for column_name in sorted(incoming_source_names - expected_source_names)
    ]

    errors = [
        ValidationIssue(message=f"Missing required column '{column.source_name}'.")
        for column in table_definition.columns
        if column.required and normalize_column_name(column.source_name) not in incoming_source_names
    ]

    kept_columns = {name: source_to_target[name] for name in df.columns if name in source_to_target}
    aligned = df[list(kept_columns)].rename(columns=kept_columns)

    for column in table_definition.columns:
        if column.target_name not in aligned.columns:
            aligned[column.target_name] = None

    return aligned, errors, warnings


def _string_series(series: pd.Series) -> pd.Series:
    """Normalize a string series."""

    return series.apply(lambda value: None if pd.isna(value) or str(value).strip() == "" else str(value).strip())


def _numeric_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse a numeric series and return parsed values plus an invalid mask."""

    raw = series.copy()
    parsed = pd.to_numeric(raw, errors="coerce")
    invalid_mask = raw.notna() & raw.astype(str).str.strip().ne("") & parsed.isna()
    return parsed, invalid_mask


def _date_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse a date series and return parsed values plus an invalid mask."""

    raw = series.copy()
    raw_text = raw.astype(str).str.strip()
    missing_mask = raw.isna() | raw_text.str.lower().isin({"", "na", "n/a", "none", "null", "nan", "nat", "-"})
    normalized = raw.mask(missing_mask, None)
    parsed = pd.to_datetime(normalized, errors="coerce").dt.date
    invalid_mask = ~missing_mask & parsed.isna()
    return parsed, invalid_mask


def _boolean_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse a boolean series and return parsed values plus an invalid mask."""

    truthy_values = {"1", "true", "t", "yes", "y", "on"}
    falsy_values = {"0", "false", "f", "no", "n", "off"}

    parsed_values: list[bool | None] = []
    invalid_mask = pd.Series(False, index=series.index)

    for row_index, value in series.items():
        if pd.isna(value) or str(value).strip() == "":
            parsed_values.append(None)
            continue

        normalized = str(value).strip().lower()
        if normalized in truthy_values:
            parsed_values.append(True)
        elif normalized in falsy_values:
            parsed_values.append(False)
        else:
            parsed_values.append(None)
            invalid_mask.loc[row_index] = True

    return pd.Series(parsed_values, index=series.index, dtype="object"), invalid_mask


def cast_columns(df: pd.DataFrame, table_definition: TableDefinition) -> tuple[pd.DataFrame, list[ValidationIssue]]:
    """Cast aligned CSV columns into their configured types."""

    issues: list[ValidationIssue] = []
    cast_df = df.copy()

    for column_definition in table_definition.columns:
        column_name = column_definition.target_name
        series = cast_df[column_name]

        if column_definition.dtype == "string":
            cast_df[column_name] = _string_series(series)
        elif column_definition.dtype == "integer":
            parsed, invalid_mask = _numeric_series(series)
            cast_df[column_name] = parsed.apply(lambda value: None if pd.isna(value) else int(value))
            for row_index in cast_df.index[invalid_mask]:
                issues.append(
                    ValidationIssue(
                        row_number=row_index + 2,
                        column_name=column_name,
                        message=f"Invalid integer value in '{column_name}'.",
                    )
                )
        elif column_definition.dtype == "float":
            parsed, invalid_mask = _numeric_series(series)
            cast_df[column_name] = parsed.apply(lambda value: None if pd.isna(value) else float(value))
            for row_index in cast_df.index[invalid_mask]:
                issues.append(
                    ValidationIssue(
                        row_number=row_index + 2,
                        column_name=column_name,
                        message=f"Invalid float value in '{column_name}'.",
                    )
                )
        elif column_definition.dtype == "date":
            parsed, invalid_mask = _date_series(series)
            cast_df[column_name] = parsed
            for row_index in cast_df.index[invalid_mask]:
                issues.append(
                    ValidationIssue(
                        row_number=row_index + 2,
                        column_name=column_name,
                        message=f"Invalid date value in '{column_name}'. Expected YYYY-MM-DD.",
                    )
                )
        elif column_definition.dtype == "boolean":
            parsed, invalid_mask = _boolean_series(series)
            cast_df[column_name] = parsed
            for row_index in cast_df.index[invalid_mask]:
                issues.append(
                    ValidationIssue(
                        row_number=row_index + 2,
                        column_name=column_name,
                        message=(
                            f"Invalid boolean value in '{column_name}'. "
                            "Expected true/false, yes/no, or 1/0."
                        ),
                    )
                )
        else:
            raise ValueError(f"Unsupported dtype '{column_definition.dtype}' for column '{column_name}'.")

        if column_definition.required:
            missing_mask = cast_df[column_name].isna()
            for row_index in cast_df.index[missing_mask]:
                issues.append(
                    ValidationIssue(
                        row_number=row_index + 2,
                        column_name=column_name,
                        message=f"Required value missing in '{column_name}'.",
                    )
                )

    return cast_df, issues


def detect_duplicate_keys(df: pd.DataFrame, unique_columns: tuple[str, ...]) -> list[ValidationIssue]:
    """Detect duplicate natural keys inside the uploaded file."""

    issues: list[ValidationIssue] = []
    if df.empty:
        return issues

    duplicate_mask = df.duplicated(subset=list(unique_columns), keep=False)
    for row_index in df.index[duplicate_mask]:
        issues.append(
            ValidationIssue(
                row_number=row_index + 2,
                message=f"Duplicate natural key found for columns {', '.join(unique_columns)}.",
            )
        )
    return issues


def validate_upload(
    table_name: str,
    file_bytes: bytes,
    session: Session,
) -> ValidationResult:
    """Validate an uploaded CSV and resolve it into load-ready rows."""

    from mielections.etl.transforms import prepare_for_load

    if table_name not in TABLE_DEFINITIONS:
        raise ValueError(f"Unsupported table: {table_name}")

    table_definition = TABLE_DEFINITIONS[table_name]
    raw_df = read_csv_bytes(file_bytes)
    aligned_df, column_errors, warnings = align_columns(raw_df, table_definition)
    cast_df, cast_errors = cast_columns(aligned_df, table_definition)

    errors = [*column_errors, *cast_errors]
    if errors:
        return ValidationResult(
            table_name=table_name,
            normalized_frame=pd.DataFrame(),
            preview_frame=cast_df.head(50),
            errors=errors,
            warnings=warnings,
        )

    prepared_df, lookup_errors = prepare_for_load(table_name, cast_df, session)
    errors.extend(lookup_errors)

    if not errors:
        errors.extend(detect_duplicate_keys(prepared_df, table_definition.upsert_columns))

    preview_columns = [column for column in table_definition.preview_columns if column in cast_df.columns]
    preview_frame = cast_df[preview_columns].head(50) if preview_columns else cast_df.head(50)

    return ValidationResult(
        table_name=table_name,
        normalized_frame=prepared_df,
        preview_frame=preview_frame,
        errors=errors,
        warnings=warnings,
    )
