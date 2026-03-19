"""Custom exceptions for ETL workflows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class UploadResult:
    """Summary of a successful upload."""

    table_name: str
    row_count: int


class EtlError(Exception):
    """Base ETL exception."""


class EtlValidationError(EtlError):
    """Raised when upload validation fails."""

    def __init__(self, message: str, issues: list) -> None:
        super().__init__(message)
        self.issues = issues
