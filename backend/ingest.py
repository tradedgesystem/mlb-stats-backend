from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from pybaseball import batting_stats

YEAR = 2023
TABLE_NAME = "batting_stats"


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


def log_missing_and_sparse(df) -> None:
    required = [
        "avg",
        "slg",
        "ops",
        "obp",
        "iso",
        "woba",
        "ops_plus",
        "wrc_plus",
        "babip",
        "h",
        "2b",
        "3b",
        "hr",
        "k",
        "bb",
        "g",
        "pa",
    ]
    normalized_required = normalize_columns(required)
    missing = sorted(set(normalized_required) - set(df.columns))
    if missing:
        print(f"Missing required columns: {', '.join(missing)}")

    sparse = df.isna().mean()
    sparse_cols = sorted(sparse[sparse > 0.2].index.tolist())
    if sparse_cols:
        print(f"Columns with >20% nulls: {', '.join(sparse_cols)}")


def main() -> None:
    df = batting_stats(YEAR)
    df.columns = normalize_columns(df.columns.tolist())

    if "season" not in df.columns:
        df["season"] = YEAR

    log_missing_and_sparse(df)

    db_path = Path(__file__).with_name("stats.db")
    with sqlite3.connect(db_path) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batting_stats_season "
            "ON batting_stats(season)"
        )
        conn.commit()

    print(f"Inserted {len(df)} rows into {db_path}")


if __name__ == "__main__":
    main()
