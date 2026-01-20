#!/usr/bin/env python3
"""Build catcher workload from MLB Stats fielding usage.

Fetches games played and innings at catcher position by mlb_id from MLB Stats API.
Computes catching_share (0..1) based on games caught vs total games played.
Writes to backend/data/catcher_workload.json (ignored in git).
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = REPO_ROOT / "backend" / "data" / "catcher_workload.json"
CACHE_DIR = REPO_ROOT / "backend" / "data" / "cache"
API_BASE = "https://statsapi.mlb.com/api/v1"
DEFAULT_SLEEP_SECONDS = 0.4
DEFAULT_TTL_HOURS = 168


def cache_is_fresh(path: Path, ttl_hours: float) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < (ttl_hours * 3600)


def load_players(players_path: Path) -> list[dict]:
    if not players_path.exists():
        return []
    with players_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("players", [])


def load_existing_workload(output_path: Path) -> dict:
    if not output_path.exists():
        return {}
    with output_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_workload(output_path: Path, data: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)


def is_catcher_position(pos: str | None) -> bool:
    if not pos:
        return False
    tokens = re.split(r"[/\s,]+", str(pos).strip().upper())
    return "C" in tokens


def fetch_catcher_stats_batch(mlb_ids: list[int], season: int) -> dict[int, dict]:
    """
    Fetch fielding stats for a batch of mlb_ids.

    Returns dict mapping mlb_id to stats including games at catcher and total games.
    """
    params = {
        "personIds": ",".join(str(mlb_id) for mlb_id in mlb_ids),
        "hydrate": f"stats(group=[fielding],type=career,season={season})",
    }
    url = f"{API_BASE}/people?{urllib.parse.urlencode(params)}"

    results: dict[int, dict] = {}
    try:
        with urllib.request.urlopen(url) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as e:
        print(f"HTTP error fetching batch: {e.code} {e.reason}")
        return results
    except Exception as e:
        print(f"Error fetching batch: {e}")
        return results

    for person in payload.get("people", []):
        mlb_id = person.get("id")
        if not isinstance(mlb_id, int):
            continue

        stats_list = person.get("stats", [])
        fielding_stats = None
        for stats in stats_list:
            if stats.get("group") == "fielding":
                fielding_stats = stats
                break

        if not fielding_stats:
            results[mlb_id] = {
                "catching_share": 0.0,
                "games_total": 0,
                "games_catching": 0,
                "source": "no_stats",
                "reason": "No fielding stats found",
            }
            continue

        splits = fielding_stats.get("splits", [])
        total_games = 0
        games_at_catcher = 0

        for split in splits:
            position = split.get("position")
            games = split.get("games", 0) or 0

            if not isinstance(position, dict):
                continue

            pos_abbrev = position.get("abbreviation", "").upper()
            total_games += games

            if pos_abbrev == "C":
                games_at_catcher = games

        if total_games > 0:
            catching_share = games_at_catcher / total_games
        else:
            catching_share = 0.0

        results[mlb_id] = {
            "catching_share": round(catching_share, 4),
            "games_total": total_games,
            "games_catching": games_at_catcher,
            "source": "mlb_stats_api",
        }

    time.sleep(DEFAULT_SLEEP_SECONDS)
    return results


def load_positions_map(positions_path: Path) -> dict[int, dict[str, str | None]]:
    """Load mlb_id-first position mapping file."""
    if not positions_path.exists():
        return {}
    with positions_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    positions: dict[int, dict[str, str | None]] = {}
    if not isinstance(data, dict):
        return positions
    for key, value in data.items():
        try:
            mlb_id = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        positions[mlb_id] = {
            "position": value.get("position"),
            "position_source": value.get("position_source"),
        }
    return positions


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build catcher workload from MLB Stats fielding data."
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Season to fetch (default: from tvp_mlb_defaults.json snapshot_year)",
    )
    parser.add_argument(
        "--positions-map",
        type=Path,
        default=REPO_ROOT / "backend" / "data" / "player_positions.json",
        help="Path to player_positions.json",
    )
    parser.add_argument(
        "--players",
        type=Path,
        default=REPO_ROOT / "backend" / "output" / "players_with_contracts_2025.json",
        help="Path to players_with_contracts_*.json",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=OUTPUT_PATH,
        help="Output path for catcher_workload.json",
    )
    parser.add_argument(
        "--ttl-hours",
        type=float,
        default=DEFAULT_TTL_HOURS,
        help="Cache TTL in hours (default: 168 = 1 week)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force refresh of cache (ignore TTL).",
    )
    parser.add_argument(
        "--max-players",
        type=int,
        default=None,
        help="Limit number of players to process (for development).",
    )
    args = parser.parse_args()

    # Determine season
    season = args.season
    if season is None:
        tvp_defaults_path = REPO_ROOT / "backend" / "tvp_mlb_defaults.json"
        if tvp_defaults_path.exists():
            with tvp_defaults_path.open("r") as handle:
                defaults = json.load(handle)
            season = defaults.get("snapshot_year")
    if season is None:
        season = 2026
        print(f"No season specified, using default: {season}")

    # Load players to get mlb_ids
    players = load_players(args.players)
    mlb_ids = [p.get("mlb_id") for p in players if isinstance(p.get("mlb_id"), int)]

    if args.max_players:
        mlb_ids = mlb_ids[: args.max_players]
        print(f"Limited to {len(mlb_ids)} players for development")

    print(f"Processing {len(mlb_ids)} players for season {season}")

    # Load positions to identify catchers
    positions_map = load_positions_map(args.positions_map)
    catcher_ids = {
        mlb_id
        for mlb_id, info in positions_map.items()
        if is_catcher_position(info.get("position"))
    }

    print(f"Found {len(catcher_ids)} catchers from positions map")

    # Check if existing workload is fresh
    if not args.refresh and cache_is_fresh(args.out, args.ttl_hours):
        print(f"Using cached workload from {args.out}")
        existing = load_existing_workload(args.out)
        if existing:
            players_section = existing.get("players", {})
            print(f"  Cached entries: {len(players_section)}")
            return

    # Fetch workload for catchers only
    catcher_mlbid_list = list(catcher_ids)
    print(f"Fetching workload for {len(catcher_mlbid_list)} catchers...")

    # Process in batches
    batch_size = 50
    players_map: dict[str, dict] = {}
    total_catchers = len(catcher_mlbid_list)

    for i in range(0, total_catchers, batch_size):
        batch = catcher_mlbid_list[i : i + batch_size]
        print(
            f"  Batch {i // batch_size + 1}/{(total_catchers - 1) // batch_size + 1}: {len(batch)} catchers"
        )
        batch_results = fetch_catcher_stats_batch(batch, season)
        for mlb_id, stats in batch_results.items():
            players_map[str(mlb_id)] = stats
        time.sleep(DEFAULT_SLEEP_SECONDS)

    # For non-catchers and catchers with no data, set catching_share=0
    all_mlbid_set = set(mlb_ids)
    processed_catchers = set(int(k) for k in players_map.keys())

    for mlb_id in mlb_ids:
        if mlb_id not in processed_catchers:
            if mlb_id not in catcher_ids:
                # Not a catcher - catching_share=0
                players_map[str(mlb_id)] = {
                    "catching_share": 0.0,
                    "games_total": 0,
                    "games_catching": 0,
                    "source": "not_catcher",
                    "reason": "Player is not a catcher by position",
                }
            else:
                # Is a catcher but no stats data - conservative default
                players_map[str(mlb_id)] = {
                    "catching_share": 1.0,
                    "games_total": 0,
                    "games_catching": 0,
                    "source": "fallback_default_1",
                    "reason": "Catcher but no fielding stats found",
                }

    # Build output
    output = {
        "meta": {
            "season": season,
            "generated_at": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "ttl_hours": args.ttl_hours,
            "total_players": len(players_map),
            "total_catchers": len(catcher_ids),
        },
        "players": players_map,
    }

    write_workload(args.out, output)
    print(f"Saved workload data to {args.out}")
    print(f"  Total entries: {len(players_map)}")
    print(
        f"  Total catchers: {len([p for p in players_map.values() if p.get('catching_share', 0) > 0])}"
    )


if __name__ == "__main__":
    main()
