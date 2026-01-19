#!/usr/bin/env python3
"""
Diagnose position coverage + catcher detection.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from compute_mlb_tvp import (  # noqa: E402
    attach_positions,
    build_catcher_ids,
    load_positions_with_fallback,
)


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
        description="Report MLB position coverage and catcher detection."
    )
    parser.add_argument(
        "--players",
        type=Path,
        default=REPO_ROOT / "backend" / "output" / "players_with_contracts_2025.json",
        help="Path to players_with_contracts_*.json",
    )
    parser.add_argument(
        "--positions",
        type=Path,
        default=REPO_ROOT / "backend" / "data" / "player_positions.json",
        help="Path to player_positions.json",
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Optional path to tvp_mlb_*.json",
    )
    args = parser.parse_args()

    players_payload = load_json(args.players)
    players = players_payload.get("players", [])

    fixture_path = REPO_ROOT / "backend" / "player_positions_fixture.json"
    positions_map = load_positions_with_fallback(args.positions, fixture_path)
    position_by_id = attach_positions(players, positions_map)
    catcher_ids = build_catcher_ids(position_by_id)

    total_players = sum(
        1 for player in players if isinstance(player.get("mlb_id"), int)
    )
    count_with_position = sum(
        1
        for info in position_by_id.values()
        if info.get("position") is not None
    )
    pct_with_position = (
        (count_with_position / total_players) * 100.0 if total_players else 0.0
    )

    print("=== POSITION COVERAGE ===")
    print(f"total_players: {total_players}")
    print(f"count_with_position: {count_with_position}")
    print(f"pct_with_position: {pct_with_position:.1f}%")

    tvp_path = args.tvp or pick_first_existing(
        [
            REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json",
            REPO_ROOT / "backend" / "output" / "tvp_mlb_2025.json",
            REPO_ROOT / "backend" / "output" / "tvp_mlb.json",
        ]
    )
    if not tvp_path:
        print("\nNo tvp_mlb output found; skipping TVP-based reports.")
        return

    tvp_data = load_json(tvp_path)
    tvp_players = tvp_data.get("players", [])

    missing_position = []
    for player in tvp_players:
        mlb_id = player.get("mlb_id")
        if not isinstance(mlb_id, int):
            continue
        info = position_by_id.get(mlb_id, {})
        if not info.get("position"):
            missing_position.append(player)

    missing_position.sort(key=lambda p: p.get("tvp_mlb") or 0.0, reverse=True)
    print("\n=== TOP 30 MISSING POSITION (BY TVP) ===")
    for player in missing_position[:30]:
        print(
            f"{player.get('mlb_id')}\t{player.get('player_name')}"
            f"\t{player.get('tvp_mlb')}"
        )

    catcher_players = [
        player
        for player in tvp_players
        if isinstance(player.get("mlb_id"), int)
        and player.get("mlb_id") in catcher_ids
    ]
    catcher_players.sort(key=lambda p: p.get("tvp_mlb") or 0.0, reverse=True)
    print("\n=== TOP 30 CATCHERS (BY TVP) ===")
    for player in catcher_players[:30]:
        mlb_id = player.get("mlb_id")
        info = position_by_id.get(mlb_id, {})
        print(
            f"{mlb_id}\t{player.get('player_name')}\t"
            f"{info.get('position')}\t{info.get('position_source')}\t"
            f"{player.get('tvp_mlb')}"
        )


if __name__ == "__main__":
    main()
