"""High-level ETL orchestration for the admin Streamlit app."""

from __future__ import annotations

from mielections.db.session import session_scope
from mielections.etl.exceptions import EtlValidationError, UploadResult
from mielections.etl.loaders import upsert_rows
from mielections.etl.validation import ValidationResult, validate_upload


def preview_upload(table_name: str, file_bytes: bytes) -> ValidationResult:
    """Validate a file and return preview data without writing to PostgreSQL."""

    with session_scope() as session:
        return validate_upload(table_name, file_bytes, session)


def execute_upload(table_name: str, file_bytes: bytes) -> UploadResult:
    """Validate and upload a CSV file inside a single transaction."""

    with session_scope() as session:
        validation = validate_upload(table_name, file_bytes, session)
        if not validation.is_valid:
            raise EtlValidationError("Upload failed validation.", validation.errors)

        row_count = upsert_rows(table_name, validation.normalized_frame, session)
        return UploadResult(table_name=table_name, row_count=row_count)
