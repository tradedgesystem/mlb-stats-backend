from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

import pandas as pd
from pybaseball import (
    statcast,
    batting_stats_bref,
    pitching_stats_bref,
    playerid_reverse_lookup,
    batting_stats_range,
    pitching_stats_range,
)

from backend.data_utils import (
    ensure_columns,
    iter_date_ranges,
    iter_dates,
    normalize_columns,
    parse_date,
)
from backend.statcast_metrics import (
    STATCAST_BATTER_COLUMNS,
    build_statcast_batter_metrics_from_df,
)
from backend.statcast_daily import (
    build_daily_batting_from_statcast,
    build_daily_pitching_from_statcast,
)
from backend.mlb_gamelogs_daily import (
    fetch_gamelogs,
)

YEAR = 2025
BAT_TABLE_NAME = "batting_stats"
PITCH_TABLE_NAME = "pitching_stats"
DAILY_BAT_TABLE_NAME = "batting_stats_daily"
DAILY_PITCH_TABLE_NAME = "pitching_stats_daily"
DATE_RANGE_START = os.environ.get("DATE_RANGE_START")
DATE_RANGE_END = os.environ.get("DATE_RANGE_END")
STATCAST_RANGE_START = os.environ.get("STATCAST_RANGE_START")
STATCAST_RANGE_END = os.environ.get("STATCAST_RANGE_END")
STATCAST_CHUNK_DAYS = int(os.environ.get("STATCAST_CHUNK_DAYS", "7"))
STATCAST_SLEEP_SECONDS = float(os.environ.get("STATCAST_SLEEP_SECONDS", "0.3"))

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

def log_missing_and_sparse(df, required: list[str]) -> None:
    normalized_required = normalize_columns(required)
    missing = sorted(set(normalized_required) - set(df.columns))
    if missing:
        print(f"Missing required columns: {', '.join(missing)}")

    sparse = df.isna().mean()
    sparse_cols = sorted(sparse[sparse > 0.2].index.tolist())
    if sparse_cols:
        print(f"Columns with >20% nulls: {', '.join(sparse_cols)}")


def fetch_statcast_data(
    start_date: datetime.date, end_date: datetime.date
) -> pd.DataFrame:
    frames = []
    for chunk_start, chunk_end in iter_date_ranges(
        start_date, end_date, STATCAST_CHUNK_DAYS
    ):
        chunk_df = statcast(chunk_start.isoformat(), chunk_end.isoformat())
        chunk_df.columns = normalize_columns(chunk_df.columns.tolist())
        frames.append(chunk_df)
        if STATCAST_SLEEP_SECONDS > 0:
            time.sleep(STATCAST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def map_batter_ids(statcast_df: pd.DataFrame) -> pd.DataFrame:
    if "batter" not in statcast_df.columns:
        return statcast_df

    batters = (
        pd.to_numeric(statcast_df["batter"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    if not batters:
        return statcast_df

    lookup = playerid_reverse_lookup(batters, key_type="mlbam")
    if lookup.empty or "key_fangraphs" not in lookup.columns:
        return statcast_df

    lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
    mapping = lookup.set_index("key_mlbam")["key_fangraphs"]
    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["batter"], errors="coerce"
    ).map(mapping)

    missing = statcast_df["player_id"].isna().sum()
    if missing:
        print(f"Statcast mapping missing FanGraphs IDs for {missing} rows.")
    return statcast_df


def build_statcast_batter_metrics(
    start_date: datetime.date, end_date: datetime.date
) -> pd.DataFrame:
    statcast_df = fetch_statcast_data(start_date, end_date)
    if statcast_df.empty:
        return pd.DataFrame(columns=["player_id"] + STATCAST_BATTER_COLUMNS)

    statcast_df = map_batter_ids(statcast_df)
    if "player_id" not in statcast_df.columns:
        return pd.DataFrame(columns=["player_id"] + STATCAST_BATTER_COLUMNS)

    return build_statcast_batter_metrics_from_df(statcast_df)


def add_player_ids(
    daily_df: pd.DataFrame, id_cache: dict[int, int]
) -> pd.DataFrame:
    if "player_id" in daily_df.columns:
        return daily_df
    if "idfg" in daily_df.columns:
        daily_df["player_id"] = daily_df["idfg"]
        return daily_df
    if "mlbid" not in daily_df.columns:
        raise ValueError("Missing expected idfg or mlbid column for player IDs.")

    mlb_ids = (
        pd.to_numeric(daily_df["mlbid"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    missing = [mlbid for mlbid in mlb_ids if mlbid not in id_cache]
    if missing:
        lookup = playerid_reverse_lookup(missing, key_type="mlbam")
        if not lookup.empty and "key_fangraphs" in lookup.columns:
            lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
            mapping = lookup.set_index("key_mlbam")["key_fangraphs"].to_dict()
            id_cache.update({int(k): int(v) for k, v in mapping.items()})

    daily_df["player_id"] = pd.to_numeric(
        daily_df["mlbid"], errors="coerce"
    ).map(id_cache)
    missing_count = daily_df["player_id"].isna().sum()
    if missing_count:
        print(f"Daily stats missing FanGraphs IDs for {missing_count} rows.")
    return daily_df


def fetch_daily_stats(day_str: str, kind: str):
    try:
        if kind == "batting":
            return batting_stats_range(day_str, day_str)
        if kind == "pitching":
            return pitching_stats_range(day_str, day_str)
    except Exception as exc:
        print(f"Skipping {kind} daily stats for {day_str}: {exc}")
        return None
    return None


def build_daily_batting(
    start_date: datetime.date,
    end_date: datetime.date,
    id_cache: dict[int, int],
    dry_run: bool = False,
) -> pd.DataFrame:
    if dry_run:
        print(f"Dry-run: Skipping MLB API calls, using cached boxscores only")
        gamelog_df = fetch_gamelogs(
            start_date.isoformat(),
            end_date.isoformat(),
            dry_run=True,
        )
    else:
        gamelog_df = fetch_gamelogs(
            start_date.isoformat(),
            end_date.isoformat(),
        )

    return build_daily_batting_from_statcast(
        start_date, end_date, gamelog_df=gamelog_df
    )


def build_daily_pitching(
    start_date: datetime.date, end_date: datetime.date, id_cache: dict[int, int]
) -> pd.DataFrame:
    return build_daily_pitching_from_statcast(start_date, end_date, id_cache=id_cache)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest MLB stats data into SQLite database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip MLB API calls, use cached data only"
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Skip writing SQLite database (compute only)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=YEAR,
        help="First season year to ingest (inclusive)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=YEAR,
        help="Last season year to ingest (inclusive)"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_year > args.end_year:
        raise ValueError("--start-year must be <= --end-year")
    years = list(range(args.start_year, args.end_year + 1))
    
    # NOTE: Fangraphs is blocked by Cloudflare (403) for all season endpoints despite User-Agent headers.
    # Using Baseball Reference as a reliable alternative that provides:
    # - Season batting/pitching stats with player IDs (mlbID)
    # - Core traditional statistics needed by the Chrome extension
    # - Advanced Fangraphs-specific metrics (WAR, wRC+, wOBA) are not available from BRef
    
    batting_frames = []
    for year in years:
        print(f"Fetching Baseball Reference batting stats for {year} season...")
        batting_df = batting_stats_bref(year)
        batting_df.columns = normalize_columns(batting_df.columns.tolist())
        if "season" not in batting_df.columns:
            batting_df["season"] = year
        batting_frames.append(batting_df)

    batting_df = pd.concat(batting_frames, ignore_index=True) if batting_frames else pd.DataFrame()
    
    # Baseball Reference returns 'mlbid' (MLB ID), we need to convert to Fangraphs ID
    # Map MLB IDs to Fangraphs IDs for compatibility with existing code
    if "mlbid" in batting_df.columns and not batting_df.empty:
        mlb_ids = (
            pd.to_numeric(batting_df["mlbid"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if mlb_ids:
            print(f"Looking up Fangraphs IDs for {len(mlb_ids)} players...")
            lookup = playerid_reverse_lookup(mlb_ids, key_type="mlbam")
            if not lookup.empty and "key_fangraphs" in lookup.columns:
                lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
                mapping = lookup.set_index("key_mlbam")["key_fangraphs"]
                batting_df["player_id"] = pd.to_numeric(
                    batting_df["mlbid"], errors="coerce"
                ).map(mapping)
                batting_df["idfg"] = batting_df["player_id"]
                missing = batting_df["player_id"].isna().sum()
                if missing:
                    print(f"Warning: Could not map Fangraphs IDs for {missing} players")
            else:
                # Fallback: use mlbid as player_id if lookup fails
                batting_df["player_id"] = pd.to_numeric(
                    batting_df["mlbid"], errors="coerce"
                )
                batting_df["idfg"] = batting_df["player_id"]
    
    if "player_id" not in batting_df.columns or "idfg" not in batting_df.columns:
        raise ValueError("Missing required player_id/idfg columns from Baseball Reference data.")

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

    if STATCAST_RANGE_START and STATCAST_RANGE_END:
        statcast_start = parse_date(STATCAST_RANGE_START)
        statcast_end = parse_date(STATCAST_RANGE_END)
        if statcast_end < statcast_start:
            raise ValueError(
                "STATCAST_RANGE_END must be on or after STATCAST_RANGE_START."
            )
        statcast_metrics = build_statcast_batter_metrics(statcast_start, statcast_end)
        if not statcast_metrics.empty:
            batting_df["player_id"] = pd.to_numeric(
                batting_df["player_id"], errors="coerce"
            )
            new_cols = [
                col
                for col in statcast_metrics.columns
                if col != "player_id" and col not in batting_df.columns
            ]
            if new_cols:
                batting_df = batting_df.merge(
                    statcast_metrics[["player_id"] + new_cols],
                    on="player_id",
                    how="left",
                )
            else:
                print("No new statcast columns to merge into batting stats.")
        else:
            print("No statcast metrics computed for the requested range.")

    log_missing_and_sparse(batting_df, REQUIRED_BATTING)
    
    pitching_frames = []
    for year in years:
        print(f"Fetching Baseball Reference pitching stats for {year} season...")
        pitching_df = pitching_stats_bref(year)
        pitching_df.columns = normalize_columns(pitching_df.columns.tolist())
        if "season" not in pitching_df.columns:
            pitching_df["season"] = year
        pitching_frames.append(pitching_df)

    pitching_df = pd.concat(pitching_frames, ignore_index=True) if pitching_frames else pd.DataFrame()
    
    # Baseball Reference returns 'mlbid' (MLB ID), convert to Fangraphs ID
    if "mlbid" in pitching_df.columns and not pitching_df.empty:
        mlb_ids = (
            pd.to_numeric(pitching_df["mlbid"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if mlb_ids:
            print(f"Looking up Fangraphs IDs for {len(mlb_ids)} pitchers...")
            lookup = playerid_reverse_lookup(mlb_ids, key_type="mlbam")
            if not lookup.empty and "key_fangraphs" in lookup.columns:
                lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
                mapping = lookup.set_index("key_mlbam")["key_fangraphs"]
                pitching_df["player_id"] = pd.to_numeric(
                    pitching_df["mlbid"], errors="coerce"
                ).map(mapping)
                pitching_df["idfg"] = pitching_df["player_id"]
                missing = pitching_df["player_id"].isna().sum()
                if missing:
                    print(f"Warning: Could not map Fangraphs IDs for {missing} pitchers")
            else:
                # Fallback: use mlbid as player_id if lookup fails
                pitching_df["player_id"] = pd.to_numeric(
                    pitching_df["mlbid"], errors="coerce"
                )
                pitching_df["idfg"] = pitching_df["player_id"]
    
    if "player_id" not in pitching_df.columns or "idfg" not in pitching_df.columns:
        raise ValueError("Missing required player_id/idfg columns from Baseball Reference data.")

    log_missing_and_sparse(pitching_df, REQUIRED_PITCHING)

    daily_batting_df = None
    daily_pitching_df = None
    if DATE_RANGE_START and DATE_RANGE_END:
        start_date = parse_date(DATE_RANGE_START)
        end_date = parse_date(DATE_RANGE_END)
        if end_date < start_date:
            raise ValueError("DATE_RANGE_END must be on or after DATE_RANGE_START.")
        id_cache: dict[int, int] = {}
        daily_batting_df = build_daily_batting(
            start_date, end_date, id_cache, dry_run=args.dry_run
        )
        daily_pitching_df = build_daily_pitching(start_date, end_date, id_cache)

    if args.no_write:
        print(
            f"No-write: computed {len(batting_df)} batting rows and "
            f"{len(pitching_df)} pitching rows; skipping SQLite write."
        )
        if daily_batting_df is not None:
            print(f"No-write: computed {len(daily_batting_df)} daily batting rows.")
        if daily_pitching_df is not None:
            print(f"No-write: computed {len(daily_pitching_df)} daily pitching rows.")
        return

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
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_batting_stats_daily_player_date "
                    "ON batting_stats_daily(player_id, game_date)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_batting_stats_daily_season_date "
                    "ON batting_stats_daily(season, game_date)"
                )
            if daily_pitching_df is not None:
                daily_pitching_df.to_sql(
                    DAILY_PITCH_TABLE_NAME, conn, if_exists="replace", index=False
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pitching_stats_daily_player_date "
                    "ON pitching_stats_daily(player_id, game_date)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pitching_stats_daily_season_date "
                    "ON pitching_stats_daily(season, game_date)"
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
