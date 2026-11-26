from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd


MAX_UPLOAD_BYTES = 25 * 1024 * 1024
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@dataclass(frozen=True)
class LoadedDataset:
    frame: pd.DataFrame
    filename: str
    extension: str
    size_bytes: int
    source_columns: tuple[str, ...]


class IngestionError(ValueError):
    """Raised when an uploaded file cannot be safely ingested."""


def load_upload(filename: str, content: bytes, max_bytes: int = MAX_UPLOAD_BYTES) -> LoadedDataset:
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise IngestionError("Upload a CSV or Excel file (.csv, .xlsx, .xls).")
    if not content:
        raise IngestionError("The uploaded file is empty.")
    if len(content) > max_bytes:
        raise IngestionError(f"File exceeds the {max_bytes // (1024 * 1024)} MB upload limit.")

    try:
        if suffix == ".csv":
            frame = pd.read_csv(BytesIO(content), dtype=str, keep_default_na=False, encoding="utf-8-sig")
        else:
            frame = pd.read_excel(BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as exc:
        raise IngestionError(f"Could not parse {suffix} input: {exc}") from exc

    if len(frame.columns) == 0:
        raise IngestionError("The file contains no header row or columns.")
    if frame.empty:
        raise IngestionError("The file contains column headers but no data records.")
    original_columns = tuple(str(column) for column in frame.columns)
    normalized = [column.strip().lower() for column in original_columns]
    if any(not column for column in normalized):
        raise IngestionError("Column names cannot be blank.")
    if len(set(normalized)) != len(normalized):
        raise IngestionError("Column names must be unique after trimming and case normalization.")
    frame.columns = normalized
    frame = frame.fillna("").astype(str)
    return LoadedDataset(frame, Path(filename).name, suffix, len(content), original_columns)
