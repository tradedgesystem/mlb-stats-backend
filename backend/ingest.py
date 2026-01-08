from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

from pybaseball import batting_stats, pitching_stats

YEAR = 2025
BAT_TABLE_NAME = "batting_stats"
PITCH_TABLE_NAME = "pitching_stats"

REQUIRED_BATTING = [
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
    "so",
    "bb",
    "g",
    "pa",
]

REQUIRED_PITCHING = [
    "era",
    "ip",
    "so",
    "bb",
    "hr",
    "whip",
    "g",
    "gs",
    "tbf",
    "h",
    "r",
    "er",
]


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


def log_missing_and_sparse(df, required: list[str]) -> None:
    normalized_required = normalize_columns(required)
    missing = sorted(set(normalized_required) - set(df.columns))
    if missing:
        print(f"Missing required columns: {', '.join(missing)}")

    sparse = df.isna().mean()
    sparse_cols = sorted(sparse[sparse > 0.2].index.tolist())
    if sparse_cols:
        print(f"Columns with >20% nulls: {', '.join(sparse_cols)}")


def main() -> None:
    batting_df = batting_stats(YEAR)
    batting_df.columns = normalize_columns(batting_df.columns.tolist())

    if "season" not in batting_df.columns:
        batting_df["season"] = YEAR
    if "idfg" not in batting_df.columns:
        raise ValueError("Missing expected idfg column for player IDs.")
    if "player_id" not in batting_df.columns:
        batting_df["player_id"] = batting_df["idfg"]

    if "barrels" in batting_df.columns and "pa" in batting_df.columns:
        batting_df["barrels_per_pa"] = batting_df["barrels"] / batting_df[
            "pa"
        ].replace(0, float("nan"))
    if "gb" in batting_df.columns and "pa" in batting_df.columns:
        batting_df["gb_per_pa"] = batting_df["gb"] / batting_df["pa"].replace(
            0, float("nan")
        )
    if "fb" in batting_df.columns and "pa" in batting_df.columns:
        batting_df["fb_per_pa"] = batting_df["fb"] / batting_df["pa"].replace(
            0, float("nan")
        )
    if "ld" in batting_df.columns and "pa" in batting_df.columns:
        batting_df["ld_per_pa"] = batting_df["ld"] / batting_df["pa"].replace(
            0, float("nan")
        )

    log_missing_and_sparse(batting_df, REQUIRED_BATTING)

    pitching_df = pitching_stats(YEAR)
    pitching_df.columns = normalize_columns(pitching_df.columns.tolist())

    if "season" not in pitching_df.columns:
        pitching_df["season"] = YEAR
    if "idfg" not in pitching_df.columns:
        raise ValueError("Missing expected idfg column for player IDs.")
    if "player_id" not in pitching_df.columns:
        pitching_df["player_id"] = pitching_df["idfg"]

    log_missing_and_sparse(pitching_df, REQUIRED_PITCHING)

    db_path = Path(__file__).with_name("stats.db")
    tmp_path = db_path.with_name("stats_tmp.db")
    if tmp_path.exists():
        tmp_path.unlink()

    try:
        with sqlite3.connect(tmp_path) as conn:
            batting_df.to_sql(BAT_TABLE_NAME, conn, if_exists="replace", index=False)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_batting_stats_season "
                "ON batting_stats(season)"
            )
            pitching_df.to_sql(
                PITCH_TABLE_NAME, conn, if_exists="replace", index=False
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pitching_stats_season "
                "ON pitching_stats(season)"
            )
            conn.commit()
        os.replace(tmp_path, db_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    print(
        f"Inserted {len(batting_df)} batting rows and {len(pitching_df)} pitching rows "
        f"into {db_path}"
    )


if __name__ == "__main__":
    main()
