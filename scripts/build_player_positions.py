#!/usr/bin/env python3
"""
Build mlb_id-first position mapping from MLB Stats API.
Output format:
  { "<mlb_id>": { "position": "C", "position_source": "mlb_stats_api" } }
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


API_BASE = "https://statsapi.mlb.com/api/v1/people"
DEFAULT_SLEEP_SECONDS = 0.4
DEFAULT_TTL_DAYS = 30


def load_players(players_path: Path) -> list[dict]:
    if not players_path.exists():
        raise FileNotFoundError(f"Players file not found: {players_path}")
    with players_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("players", [])


def load_existing_positions(output_path: Path) -> dict[int, dict]:
    if not output_path.exists():
        return {}
    with output_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    positions: dict[int, dict] = {}
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
            "position_source": value.get("position_source") or "mlb_stats_api",
        }
    return positions


def write_positions(output_path: Path, positions: dict[int, dict]) -> None:
    serialized = {str(mlb_id): data for mlb_id, data in sorted(positions.items())}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")


def fetch_positions_batch(mlb_ids: list[int]) -> dict[int, dict]:
    params = {
        "personIds": ",".join(str(mlb_id) for mlb_id in mlb_ids),
        "hydrate": "primaryPosition",
    }
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url) as response:
        payload = json.load(response)
    results: dict[int, dict] = {}
    for person in payload.get("people", []):
        mlb_id = person.get("id")
        if not isinstance(mlb_id, int):
            continue
        primary = person.get("primaryPosition") or {}
        position = primary.get("abbreviation") or primary.get("name")
        results[mlb_id] = {
            "position": position,
            "position_source": "mlb_stats_api",
        }
    return results


def cache_is_fresh(path: Path, ttl_days: int) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < (ttl_days * 86400)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build mlb_id-first player position map from MLB Stats API."
    )
    parser.add_argument(
        "--players",
        type=Path,
        default=Path("backend/output/players_with_contracts_2025.json"),
        help="Path to players_with_contracts_*.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("backend/data/player_positions.json"),
        help="Output path for player_positions.json",
    )
    parser.add_argument(
        "--mlb-ids",
        type=str,
        help="Optional comma-separated mlb_ids to fetch (overrides --players).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Rebuild mapping from scratch instead of using cached output.",
    )
    parser.add_argument(
        "--ttl-days",
        type=int,
        default=DEFAULT_TTL_DAYS,
        help="Cache TTL in days before re-fetching positions.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Seconds to sleep between API requests.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of mlb_ids per API request.",
    )
    args = parser.parse_args()

    if args.mlb_ids:
        raw_ids = [item.strip() for item in args.mlb_ids.split(",") if item.strip()]
        mlb_ids = sorted({int(item) for item in raw_ids})
    else:
        players = load_players(args.players)
        mlb_ids = sorted(
            {
                player.get("mlb_id")
                for player in players
                if isinstance(player.get("mlb_id"), int)
            }
        )

    positions = {} if args.refresh else load_existing_positions(args.output)
    missing = [mlb_id for mlb_id in mlb_ids if mlb_id not in positions]
    cache_fresh = (not args.refresh) and cache_is_fresh(args.output, args.ttl_days)

    print(f"Total players with mlb_id: {len(mlb_ids)}")
    print(f"Cached positions: {len(positions)}")
    print(f"Missing positions to fetch: {len(missing)}")

    if cache_fresh:
        print(
            f"Cache is fresh (<{args.ttl_days} days); skipping API refresh.",
        )
    elif missing:
        for idx in range(0, len(missing), args.batch_size):
            batch = missing[idx : idx + args.batch_size]
            try:
                results = fetch_positions_batch(batch)
            except urllib.error.URLError as exc:
                print(f"Error fetching MLB Stats API: {exc}")
                print("Stopping to avoid repeated failures.")
                break
            positions.update(results)
            time.sleep(max(args.sleep, 0.0))

    write_positions(args.output, positions)
    print(f"Wrote {args.output}")
    for mlb_id in mlb_ids:
        info = positions.get(mlb_id)
        if not info:
            print(f"{mlb_id}\tMISSING")
            continue
        print(f"{mlb_id}\t{info.get('position')}\t{info.get('position_source')}")


if __name__ == "__main__":
    main()
