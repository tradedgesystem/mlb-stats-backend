from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pybaseball import batting_stats, batting_stats_range, pitching_stats, pitching_stats_range

YEAR = 2025
BAT_TABLE_NAME = "batting_stats"
PITCH_TABLE_NAME = "pitching_stats"
DAILY_BAT_TABLE_NAME = "batting_stats_daily"
DAILY_PITCH_TABLE_NAME = "pitching_stats_daily"
DATE_RANGE_START = os.environ.get("DATE_RANGE_START")
DATE_RANGE_END = os.environ.get("DATE_RANGE_END")

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

DAILY_BATTING_COLUMNS = [
    "pa",
    "ab",
    "h",
    "1b",
    "2b",
    "3b",
    "hr",
    "r",
    "rbi",
    "bb",
    "ibb",
    "hbp",
    "so",
    "sf",
    "sh",
]

DAILY_PITCHING_COLUMNS = [
    "ip",
    "tbf",
    "h",
    "r",
    "er",
    "hr",
    "bb",
    "hbp",
    "so",
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


def parse_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start_date: datetime.date, end_date: datetime.date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def build_daily_batting(start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    frames = []
    for day in iter_dates(start_date, end_date):
        day_str = day.isoformat()
        daily_df = batting_stats_range(day_str, day_str)
        daily_df.columns = normalize_columns(daily_df.columns.tolist())

        if "season" not in daily_df.columns:
            daily_df["season"] = day.year
        if "idfg" not in daily_df.columns:
            raise ValueError("Missing expected idfg column for player IDs.")
        if "player_id" not in daily_df.columns:
            daily_df["player_id"] = daily_df["idfg"]
        daily_df["game_date"] = day_str

        daily_df = ensure_columns(daily_df, DAILY_BATTING_COLUMNS)
        base_cols = ["player_id", "name", "team", "season", "game_date"]
        daily_df = daily_df[base_cols + DAILY_BATTING_COLUMNS]
        frames.append(daily_df)

    if not frames:
        columns = ["player_id", "name", "team", "season", "game_date"] + DAILY_BATTING_COLUMNS
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def build_daily_pitching(start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    frames = []
    for day in iter_dates(start_date, end_date):
        day_str = day.isoformat()
        daily_df = pitching_stats_range(day_str, day_str)
        daily_df.columns = normalize_columns(daily_df.columns.tolist())

        if "season" not in daily_df.columns:
            daily_df["season"] = day.year
        if "idfg" not in daily_df.columns:
            raise ValueError("Missing expected idfg column for player IDs.")
        if "player_id" not in daily_df.columns:
            daily_df["player_id"] = daily_df["idfg"]
        daily_df["game_date"] = day_str

        daily_df = ensure_columns(daily_df, DAILY_PITCHING_COLUMNS)
        base_cols = ["player_id", "name", "team", "season", "game_date"]
        daily_df = daily_df[base_cols + DAILY_PITCHING_COLUMNS]
        frames.append(daily_df)

    if not frames:
        columns = ["player_id", "name", "team", "season", "game_date"] + DAILY_PITCHING_COLUMNS
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    batting_df = batting_stats(YEAR, qual=0)
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

    pitching_df = pitching_stats(YEAR, qual=0)
    pitching_df.columns = normalize_columns(pitching_df.columns.tolist())

    if "season" not in pitching_df.columns:
        pitching_df["season"] = YEAR
    if "idfg" not in pitching_df.columns:
        raise ValueError("Missing expected idfg column for player IDs.")
    if "player_id" not in pitching_df.columns:
        pitching_df["player_id"] = pitching_df["idfg"]

    log_missing_and_sparse(pitching_df, REQUIRED_PITCHING)

    daily_batting_df = None
    daily_pitching_df = None
    if DATE_RANGE_START and DATE_RANGE_END:
        start_date = parse_date(DATE_RANGE_START)
        end_date = parse_date(DATE_RANGE_END)
        if end_date < start_date:
            raise ValueError("DATE_RANGE_END must be on or after DATE_RANGE_START.")
        daily_batting_df = build_daily_batting(start_date, end_date)
        daily_pitching_df = build_daily_pitching(start_date, end_date)

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
            if daily_batting_df is not None:
                daily_batting_df.to_sql(
                    DAILY_BAT_TABLE_NAME, conn, if_exists="replace", index=False
                )
            if daily_pitching_df is not None:
                daily_pitching_df.to_sql(
                    DAILY_PITCH_TABLE_NAME, conn, if_exists="replace", index=False
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
