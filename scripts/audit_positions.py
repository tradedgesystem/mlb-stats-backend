#!/usr/bin/env python3
"""
Audit MLB position coverage and catcher distribution from TVP output.
Reports separate metrics for missing position vs missing projection.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def pick_first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit MLB position coverage and catcher distribution."
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Path to tvp_mlb_*.json (default: latest in backend/output).",
    )
    parser.add_argument(
        "--positions",
        type=Path,
        default=REPO_ROOT / "backend" / "data" / "player_positions.json",
        help="Path to player_positions.json (for coverage checks).",
    )
    args = parser.parse_args()

    tvp_path = args.tvp or pick_first_existing(
        [
            REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json",
            REPO_ROOT / "backend" / "output" / "tvp_mlb_2025.json",
            REPO_ROOT / "backend" / "output" / "tvp_mlb.json",
        ]
    )
    if not tvp_path or not tvp_path.exists():
        raise SystemExit("No tvp_mlb output found under backend/output.")

    # Load TVP data
    data = load_json(tvp_path)
    players = data.get("players", [])
    total_players = len(players)

    # Load positions map
    positions_map = {}
    if args.positions.exists():
        positions_data = load_json(args.positions)
        for key, value in positions_data.items():
            try:
                mlb_id = int(key)
                positions_map[mlb_id] = value
            except (ValueError, TypeError):
                continue

    def projection(player: dict) -> dict:
        return player.get("raw_components", {}).get("projection", {})

    # Count players with projection
    players_with_projection = [p for p in players if projection(p)]
    count_with_projection = len(players_with_projection)

    # Count players with/without position
    with_position = [
        p for p in players_with_projection if projection(p).get("position")
    ]
    count_with_position = len(with_position)
    missing_position_in_projected = [
        p for p in players_with_projection if not projection(p).get("position")
    ]
    count_missing_position_in_projected = len(missing_position_in_projected)

    # Count players with/without mlb_id
    players_with_mlb_id = [p for p in players_with_projection if p.get("mlb_id")]
    count_with_mlb_id = len(players_with_mlb_id)
    missing_mlb_id_in_projected = [
        p for p in players_with_projection if not p.get("mlb_id")
    ]
    count_missing_mlb_id_in_projected = len(missing_mlb_id_in_projected)

    # Check positions map coverage among players with mlb_id
    mlb_ids_with_projection = set(
        p["mlb_id"] for p in players_with_mlb_id if p.get("mlb_id")
    )
    ids_in_positions_map = set(positions_map.keys())
    ids_missing_from_positions_map = mlb_ids_with_projection - ids_in_positions_map
    count_ids_missing_from_positions_map = len(ids_missing_from_positions_map)

    # Among players in positions map, check for missing position data
    count_with_mlb_id_and_position = 0
    count_mlb_id_missing_position = 0
    for mlb_id in mlb_ids_with_projection:
        if mlb_id in positions_map:
            pos_info = positions_map[mlb_id]
            if pos_info.get("position"):
                count_with_mlb_id_and_position += 1
            else:
                count_mlb_id_missing_position += 1

    positions_map_coverage = (
        count_with_mlb_id_and_position / len(mlb_ids_with_projection) * 100.0
        if len(mlb_ids_with_projection) > 0
        else 0.0
    )

    # Count catchers
    catchers = [
        p for p in players_with_projection if projection(p).get("is_catcher") is True
    ]
    non_catchers = [
        p for p in players_with_projection if projection(p).get("is_catcher") is False
    ]

    pct_with_position = (
        (len(with_position) / total_players * 100.0) if total_players else 0.0
    )
    pct_catchers = (len(catchers) / total_players * 100.0) if total_players else 0.0

    def tvp_current(player: dict) -> float:
        value = player.get("tvp_current")
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    catchers_sorted = sorted(catchers, key=tvp_current, reverse=True)[:20]
    non_catchers_sorted = sorted(non_catchers, key=tvp_current, reverse=True)[:20]

    # Collect error codes from raw_components
    error_reasons = {}
    for player in players_with_projection:
        proj = projection(player)
        error = proj.get("error")
        if error:
            error_reasons[error] = error_reasons.get(error, 0) + 1

    print("=== PLAYER COVERAGE SUMMARY ===")
    print(f"total_players_in_output: {total_players}")
    print(
        f"count_with_projection: {count_with_projection} ({count_with_projection / total_players * 100:.1f}%)"
    )
    print()
    print("=== PROJECTION STATUS ===")
    print(f"count_with_projection_and_position: {count_with_position}")
    print(
        f"count_with_projection_missing_position: {count_missing_position_in_projected}"
    )
    print(f"pct_with_position: {pct_with_position:.1f}%")
    print()
    print("=== MLB ID STATUS ===")
    print(f"count_with_projection_and_mlb_id: {count_with_mlb_id}")
    print(f"count_with_projection_missing_mlb_id: {count_missing_mlb_id_in_projected}")
    print()
    print("=== POSITIONS MAP COVERAGE ===")
    print(f"ids_in_positions_map: {len(ids_in_positions_map)}")
    print(f"ids_missing_from_positions_map: {count_ids_missing_from_positions_map}")
    print(f"positions_map_coverage: {positions_map_coverage:.1f}%")
    print(f"  (among projected players with mlb_id)")
    print(f"  count_with_mlb_id_and_position: {count_with_mlb_id_and_position}")
    print(f"  count_mlb_id_missing_position: {count_mlb_id_missing_position}")
    print()

    if error_reasons:
        print("=== ERROR BREAKDOWN ===")
        for error, count in sorted(error_reasons.items()):
            print(f"  {error}: {count}")

    print("=== CATCHER STATS ===")
    print(f"count_catchers: {len(catchers)}")
    print(f"pct_catchers: {pct_catchers:.2f}%")

    print("\n=== TOP 20 CATCHERS BY TVP_CURRENT ===")
    for player in catchers_sorted:
        proj = projection(player)
        print(
            f"{player.get('player_name')}\t{player.get('mlb_id')}\t"
            f"{proj.get('position')}\t{player.get('tvp_current')}"
        )

    print("\n=== TOP 20 NON-CATCHERS BY TVP_CURRENT ===")
    for player in non_catchers_sorted:
        proj = projection(player)
        print(
            f"{player.get('player_name')}\t{player.get('mlb_id')}\t"
            f"{proj.get('position')}\t{player.get('tvp_current')}"
        )

    # Exit with error if coverage issues
    exit_code = 0
    if positions_map_coverage < 99.0:
        print(
            f"\n❌ ERROR: positions_map_coverage ({positions_map_coverage:.1f}%) < 99%"
        )
        exit_code = 1
    if count_ids_missing_from_positions_map > 0:
        print(
            f"\n❌ ERROR: {count_ids_missing_from_positions_map} projected players missing from positions_map"
        )
        exit_code = 1
    if count_mlb_id_and_position > 0:
        projected_plus_position_missing = (
            count_mlb_id_missing_position / count_with_mlb_id_and_position * 100.0
        )
        print(
            f"\n⚠️  WARNING: {projected_plus_position_missing:.1f}% of projected players with mlb_id are missing position data"
        )

    if exit_code != 0:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
