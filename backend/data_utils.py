from __future__ import annotations

import re
from datetime import datetime, timedelta


def normalize_columns(columns: list[str]) -> list[str]:
    normalized: list[str] = []
    for col in columns:
        col = col.strip().lower()
        col = col.replace("%", "pct")
        col = col.replace("+", "_plus")
        col = re.sub(r"[\s/\-]+", "_", col)
        col = re.sub(r"[^a-z0-9_]", "", col)
        col = re.sub(r"_+", "_", col).strip("_")
        normalized.append(col or "col")

    counts: dict[str, int] = {}
    unique: list[str] = []
    for col in normalized:
        count = counts.get(col, 0) + 1
        counts[col] = count
        unique.append(col if count == 1 else f"{col}_{count}")
    return unique


def parse_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start_date: datetime.date, end_date: datetime.date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def iter_date_ranges(
    start_date: datetime.date, end_date: datetime.date, chunk_days: int
):
    if chunk_days < 1:
        raise ValueError("STATCAST_CHUNK_DAYS must be at least 1.")
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def ensure_columns(df, columns: list[str]):
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df
