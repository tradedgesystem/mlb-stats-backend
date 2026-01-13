from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
from pybaseball import playerid_reverse_lookup, statcast

try:
    from .data_utils import iter_date_ranges, iter_dates, normalize_columns, parse_date
except ImportError:  # pragma: no cover - script execution fallback
    from data_utils import iter_date_ranges, iter_dates, normalize_columns, parse_date


def map_batter_ids(statcast_df: pd.DataFrame, id_cache: dict[int, int]) -> pd.DataFrame:
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

    missing = [batter for batter in batters if batter not in id_cache]
    if missing:
        lookup = playerid_reverse_lookup(missing, key_type="mlbam")
        if not lookup.empty and "key_fangraphs" in lookup.columns:
            lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
            mapping = lookup.set_index("key_mlbam")["key_fangraphs"]
            id_cache.update(mapping.to_dict())

    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["batter"], errors="coerce"
    ).map(id_cache)
    return statcast_df


def get_output_path(base_dir: Path, season: int, day: str) -> Path:
    return base_dir / f"season={season}" / f"game_date={day}" / "statcast.parquet"


def chunk_needs_fetch(base_dir: Path, season: int, start_date, end_date) -> bool:
    for day in iter_dates(start_date, end_date):
        output_path = get_output_path(base_dir, season, day.isoformat())
        if not output_path.exists():
            return True
    return False


def write_statcast_chunk(
    statcast_df: pd.DataFrame,
    base_dir: Path,
    season: int,
    overwrite: bool,
) -> int:
    if statcast_df.empty:
        return 0

    if "game_date" not in statcast_df.columns:
        raise ValueError("Statcast payload is missing game_date.")

    statcast_df["game_date"] = pd.to_datetime(
        statcast_df["game_date"], errors="coerce"
    )
    statcast_df = statcast_df[statcast_df["game_date"].notna()].copy()
    statcast_df["game_date"] = statcast_df["game_date"].dt.date.astype(str)

    written = 0
    for day, day_df in statcast_df.groupby("game_date"):
        output_path = get_output_path(base_dir, season, day)
        if output_path.exists() and not overwrite:
            continue
        output_path.parent.mkdir(parents=True, exist_ok=True)
        day_df.to_parquet(output_path, index=False)
        written += len(day_df)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill raw Statcast pitch data to Parquet files."
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--chunk-days", type=int, default=7, help="Chunk size for Statcast calls."
    )
    parser.add_argument(
        "--sleep", type=float, default=1.0, help="Seconds to sleep between calls."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "statcast" / "raw",
        help="Base output directory for Parquet files.",
    )
    parser.add_argument("--season", type=int, help="Season year (defaults to start).")
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files."
    )
    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if end_date < start_date:
        raise ValueError("End date must be on or after start date.")

    season = args.season or start_date.year
    base_dir = args.output

    id_cache: dict[int, int] = {}
    total_written = 0
    for chunk_start, chunk_end in iter_date_ranges(
        start_date, end_date, args.chunk_days
    ):
        if not args.overwrite and not chunk_needs_fetch(
            base_dir, season, chunk_start, chunk_end
        ):
            continue

        chunk_df = statcast(chunk_start.isoformat(), chunk_end.isoformat())
        if chunk_df.empty:
            continue
        chunk_df.columns = normalize_columns(chunk_df.columns.tolist())
        chunk_df = map_batter_ids(chunk_df, id_cache)
        if "player_id" in chunk_df.columns:
            chunk_df = chunk_df[chunk_df["player_id"].notna()].copy()
            chunk_df["player_id"] = chunk_df["player_id"].astype(int)

        total_written += write_statcast_chunk(
            chunk_df, base_dir, season, args.overwrite
        )
        if args.sleep > 0:
            time.sleep(args.sleep)

    print(f"Wrote {total_written} Statcast rows to {base_dir}")


if __name__ == "__main__":
    main()
