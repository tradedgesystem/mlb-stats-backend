#!/usr/bin/env python3
"""
Audit MLB position coverage and catcher distribution from TVP output.
"""
from __future__ import annotations

import argparse
import json
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

    data = load_json(tvp_path)
    players = data.get("players", [])
    total_players = len(players)

    def projection(player: dict) -> dict:
        return player.get("raw_components", {}).get("projection", {})

    with_position = [p for p in players if projection(p).get("position")]
    missing_positions = [p for p in players if not projection(p).get("position")]
    catchers = [p for p in players if projection(p).get("is_catcher") is True]
    non_catchers = [p for p in players if projection(p).get("is_catcher") is False]

    pct_with_position = (len(with_position) / total_players * 100.0) if total_players else 0.0
    pct_catchers = (len(catchers) / total_players * 100.0) if total_players else 0.0

    def tvp_current(player: dict) -> float:
        value = player.get("tvp_current")
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    catchers_sorted = sorted(catchers, key=tvp_current, reverse=True)[:20]
    non_catchers_sorted = sorted(non_catchers, key=tvp_current, reverse=True)[:20]

    print("=== POSITION COVERAGE ===")
    print(f"total_players: {total_players}")
    print(f"count_with_position: {len(with_position)}")
    print(f"pct_with_position: {pct_with_position:.1f}%")
    print(f"count_missing_positions: {len(missing_positions)}")
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


if __name__ == "__main__":
    main()
