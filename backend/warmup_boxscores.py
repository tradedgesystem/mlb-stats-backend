#!/usr/bin/env python3
"""
Warmup script for pre-fetching MLB boxscore data.

This script fetches and caches boxscore JSON files for a date range,
allowing for faster subsequent data processing.
"""

import argparse
import json
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from mlb_gamelogs_daily import (
    COMPLETED_GAME_STATES,
    SCHEDULE_URL,
    BOXSCORE_URL,
    get_cache_dir,
)


MANIFEST_FILENAME = "manifest.json"


def load_manifest(cache_dir: Path) -> dict[str, Any]:
    path = cache_dir / "boxscore" / MANIFEST_FILENAME
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_manifest(cache_dir: Path, manifest: dict[str, Any]) -> None:
    path = cache_dir / "boxscore" / MANIFEST_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["last_updated"] = datetime.utcnow().isoformat() + "Z"
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def count_cached_boxscores(cache_dir: Path) -> int:
    boxscore_dir = cache_dir / "boxscore"
    if not boxscore_dir.exists():
        return 0
    return sum(
        1
        for p in boxscore_dir.iterdir()
        if p.is_file() and p.suffix == ".json" and p.name != MANIFEST_FILENAME
    )


def reset_cache(cache_dir: Path) -> None:
    boxscore_dir = cache_dir / "boxscore"
    if boxscore_dir.exists():
        print(f"Removing all boxscore cache files from {boxscore_dir}")
        shutil.rmtree(boxscore_dir)
        boxscore_dir.mkdir(parents=True, exist_ok=True)
        print(f"Boxscore cache reset complete")


def reset_manifest(cache_dir: Path) -> None:
    manifest_path = cache_dir / "boxscore" / MANIFEST_FILENAME
    if manifest_path.exists():
        print(f"Removing manifest file: {manifest_path}")
        manifest_path.unlink()
        print(f"Manifest reset complete")


def fetch_schedule_for_day(date_str: str) -> list[dict[str, Any]]:
    params = {
        "sportId": "1",
        "startDate": date_str,
        "endDate": date_str,
    }
    response = __import__("requests").get(SCHEDULE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "dates" not in data or not data["dates"]:
        return []

    date_data = data["dates"][0]
    if "games" not in date_data:
        return []

    completed_games = []
    for game in date_data["games"]:
        status = game.get("status", {})
        detailed_state = status.get("detailedState", "")
        if detailed_state in COMPLETED_GAME_STATES:
            completed_games.append(game)

    return completed_games


def warmup_day(
    date_str: str,
    cache_dir: Path,
    sleep_seconds: float = 1.0,
    dry_run: bool = False,
) -> dict[str, int]:
    boxscore_dir = cache_dir / "boxscore"
    boxscore_dir.mkdir(parents=True, exist_ok=True)

    games = fetch_schedule_for_day(date_str)
    if not games:
        return {"total": 0, "fetched": 0, "cached": 0}

    game_pks = [game["gamePk"] for game in games]
    total = len(game_pks)
    fetched = 0
    cached = 0

    requests = __import__("requests")

    for game_pk in game_pks:
        cache_path = boxscore_dir / f"{game_pk}.json"

        if cache_path.exists():
            cached += 1
            continue

        if dry_run:
            print(f"Dry-run: Would fetch boxscore for game {game_pk}")
            continue

        try:
            url = BOXSCORE_URL.format(game_pk)
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            with open(cache_path, "w") as f:
                json.dump(data, f)

            fetched += 1
        except Exception as exc:
            print(f"Error fetching boxscore for game {game_pk}: {exc}")
            raise

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {"total": total, "fetched": fetched, "cached": cached}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Warmup MLB boxscore cache by fetching games in date range"
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Sleep seconds between API calls (default: 1.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched without making API calls",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last completed date in manifest",
    )
    parser.add_argument(
        "--reset-manifest",
        action="store_true",
        help="Reset manifest file before starting",
    )
    parser.add_argument(
        "--reset-cache",
        action="store_true",
        help="Remove all cached boxscore files before starting",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Cache directory (default: data/mlb_api, or CACHE_DIR env var)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    except ValueError:
        print("Error: --start must be in YYYY-MM-DD format")
        return 1

    try:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        print("Error: --end must be in YYYY-MM-DD format")
        return 1

    if end_date < start_date:
        print("Error: --end must be on or after --start")
        return 1

    cache_dir = get_cache_dir(args.cache_dir)

    if args.reset_cache:
        reset_cache(cache_dir)

    if args.reset_manifest:
        reset_manifest(cache_dir)

    manifest = load_manifest(cache_dir)

    resume_date = None
    if args.resume and "last_completed_date" in manifest:
        try:
            resume_date = datetime.strptime(
                manifest["last_completed_date"], "%Y-%m-%d"
            ).date()
            resume_date = resume_date + timedelta(days=1)
        except ValueError:
            print("Warning: Invalid last_completed_date in manifest, starting from --start")
            resume_date = start_date

    actual_start = max(start_date, resume_date) if resume_date else start_date

    if actual_start != start_date:
        print(f"Resuming from {actual_start} (original start: {start_date})")

    start_time = datetime.now()
    total_fetched = 0
    total_cached = 0
    total_games = 0

    current_date = actual_start
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f"\nProcessing {date_str}...")

        try:
            result = warmup_day(
                date_str,
                cache_dir,
                sleep_seconds=args.sleep,
                dry_run=args.dry_run,
            )

            total_games += result["total"]
            total_fetched += result["fetched"]
            total_cached += result["cached"]

            cached_count = count_cached_boxscores(cache_dir)

            print(
                f"{date_str}: {result['fetched']}/{result['total']} games fetched "
                f"({result['cached']} cached) "
                f"(total cached: {cached_count})"
            )

            manifest["last_completed_date"] = date_str
            manifest["total_cached"] = cached_count
            save_manifest(cache_dir, manifest)

        except Exception as exc:
            print(f"Error processing {date_str}: {exc}")
            print("Stopping. Use --resume to continue from this date.")
            return 1

        current_date += timedelta(days=1)

    final_cached = count_cached_boxscores(cache_dir)
    elapsed = datetime.now() - start_time

    print("\n" + "=" * 60)
    print("Warmup complete!")
    print(f"Total games processed: {total_games}")
    print(f"Total fetched: {total_fetched}")
    print(f"Total skipped (cached): {total_cached}")
    print(f"Total cached boxscores: {final_cached}")
    print(f"Elapsed: {elapsed}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
