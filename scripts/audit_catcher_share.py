#!/usr/bin/env python3
"""
Audit catcher share in Top 50 TVP_CURRENT list.
Tests whether catcher representation stays within acceptable bounds (max 10%).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVP_PATH = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"
MAX_CATCHER_SHARE_PCT = 10.0


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
        description="Audit catcher share in Top 50 TVP_CURRENT."
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Path to tvp_mlb_*.json (default: latest in backend/output).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Number of top players to check (default: 50).",
    )
    parser.add_argument(
        "--max-share",
        type=float,
        default=MAX_CATCHER_SHARE_PCT,
        help=f"Maximum acceptable catcher share percentage (default: {MAX_CATCHER_SHARE_PCT}).",
    )
    parser.add_argument(
        "--fail-on-exceed",
        action="store_true",
        help="Exit with error code if catcher share exceeds maximum.",
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

    def tvp_current(player: dict) -> float:
        value = player.get("tvp_current")
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    valid_players = [p for p in players if tvp_current(p) is not None]
    sorted_players = sorted(valid_players, key=tvp_current, reverse=True)
    top_n = sorted_players[: args.top_n]

    catchers_in_top_n = [p for p in top_n if projection(p).get("is_catcher") is True]
    non_catchers_in_top_n = [
        p for p in top_n if projection(p).get("is_catcher") is False
    ]

    catcher_count = len(catchers_in_top_n)
    total_top_n = len(top_n)
    catcher_share_pct = (catcher_count / total_top_n * 100.0) if total_top_n else 0.0

    print(f"=== CATCHER SHARE AUDIT (Top {args.top_n}) ===")
    print(f"Total players: {total_players}")
    print(f"Top {args.top_n} examined: {total_top_n}")
    print(
        f"Catchers in Top {args.top_n}: {catcher_count}/{total_top_n} ({catcher_share_pct:.1f}%)"
    )
    print(f"Maximum acceptable: {args.max_share:.1f}%")
    print()

    passes = catcher_share_pct <= args.max_share
    status_icon = "✓" if passes else "✗"
    print(f"{status_icon} STATUS: {'PASS' if passes else 'FAIL'}")

    if catchers_in_top_n:
        print(f"\n=== CATCHERS IN TOP {args.top_n} (by TVP_CURRENT) ===")
        print(
            f"{'Rank':<6} {'Player':<25} {'MLB ID':<10} {'TVP_CURRENT':<15} {'Adjustments'}"
        )
        print("-" * 80)

        for player in catchers_in_top_n:
            rank = sorted_players.index(player) + 1
            name = player.get("player_name", "N/A")
            mlb_id = player.get("mlb_id", "N/A")
            tvp = tvp_current(player)

            proj = projection(player)
            catcher_risk = proj.get("catcher_risk_adjustments", {})
            if catcher_risk:
                last_season_adjustment = catcher_risk.get(
                    max(catcher_risk.keys(), default=None), {}
                )
                adjustments = (
                    f"avail={last_season_adjustment.get('availability_discount', 1.0):.2f}, "
                    f"decline={last_season_adjustment.get('steeper_mult', 1.0):.2f}, "
                    f"pos_chg={last_season_adjustment.get('position_change_prob', 0.0):.2f}"
                )
            else:
                adjustments = "N/A"

            print(
                f"{rank:<6} {name:<25} {mlb_id if isinstance(mlb_id, int) else 'N/A':<10} {tvp:<15.3f} {adjustments}"
            )

    if not passes and args.fail_on_exceed:
        print(
            f"\n❌ ERROR: Catcher share ({catcher_share_pct:.1f}%) exceeds maximum ({args.max_share:.1f}%)"
        )
        sys.exit(1)

    print()
    print("=== SUMMARY ===")
    print(f"Target: 5-7% catcher representation in Top {args.top_n}")
    print(f"Actual: {catcher_share_pct:.1f}%")
    if passes:
        print(f"Result: Within acceptable bounds ✓")
    else:
        print(f"Result: Exceeds acceptable bounds ✗")


if __name__ == "__main__":
    main()
