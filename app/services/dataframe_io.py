from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


CSV_SUFFIXES = {".csv"}
EXCEL_SUFFIXES = {".xlsx", ".xlsm", ".xltx", ".xltm"}
PARQUET_SUFFIXES = {".parquet"}
TABULAR_SUFFIXES = CSV_SUFFIXES | EXCEL_SUFFIXES | PARQUET_SUFFIXES
CSV_ENCODINGS: Iterable[str] = ("utf-8", "utf-8-sig", "gbk", "gb18030")


def is_excel_path(path: str | Path) -> bool:
    return Path(path).suffix.lower() in EXCEL_SUFFIXES


def is_tabular_path(path: str | Path) -> bool:
    return Path(path).suffix.lower() in TABULAR_SUFFIXES


def load_dataframe(dataset_path: str | Path) -> pd.DataFrame:
    path = Path(dataset_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {path}")

    suffix = path.suffix.lower()

    if suffix in CSV_SUFFIXES:
        csv_errors = []
        for encoding in CSV_ENCODINGS:
            try:
                return pd.read_csv(path, encoding=encoding)
            except Exception as exc:
                csv_errors.append(f"{encoding}: {exc}")
        raise ValueError(
            "Failed to read CSV with supported encodings. "
            f"Details: {' | '.join(csv_errors)}"
        )

    if suffix in EXCEL_SUFFIXES:
        try:
            return pd.read_excel(path)
        except ImportError as exc:
            raise ImportError(
                "Reading Excel files requires the 'openpyxl' package."
            ) from exc

    if suffix in PARQUET_SUFFIXES:
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported dataset format: {path}")
