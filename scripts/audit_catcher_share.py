#!/usr/bin/env python3
"""
Audit catcher share in Top N TVP_CURRENT list.
Tests whether catcher representation stays within acceptable bounds.
Supports multiple Top N values and band-based target ranges.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVP_PATH = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"


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
        description="Audit catcher share in Top N TVP_CURRENT."
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Path to tvp_mlb_*.json (default: latest in backend/output).",
    )
    parser.add_argument(
        "--topn",
        type=int,
        default=50,
        help="Number of top players to check (default: 50).",
    )
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="Target range for catcher share (e.g., '0.05,0.07' for 5-7%).",
    )
    parser.add_argument(
        "--min-pct",
        type=float,
        default=5.0,
        help="Minimum acceptable catcher share percentage (default: 5.0).",
    )
    parser.add_argument(
        "--max-pct",
        type=float,
        default=7.0,
        help="Maximum acceptable catcher share percentage (default: 7.0).",
    )
    parser.add_argument(
        "--fail-on-outside-band",
        action="store_true",
        help="Exit with error code if catcher share is outside target band.",
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
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    valid_players = [p for p in players if tvp_current(p) is not None]
    sorted_players = sorted(valid_players, key=tvp_current, reverse=True)
    top_n = sorted_players[: args.topn]

    catchers_in_top_n = [p for p in top_n if projection(p).get("is_catcher") is True]
    non_catchers_in_top_n = [
        p for p in top_n if projection(p).get("is_catcher") is False
    ]

    catcher_count = len(catchers_in_top_n)
    total_top_n = len(top_n)
    catcher_share_pct = (catcher_count / total_top_n * 100.0) if total_top_n else 0.0

    # Parse target band
    min_target = args.min_pct
    max_target = args.max_pct
    if args.target:
        try:
            parts = args.target.split(",")
            min_target = float(parts[0].strip())
            max_target = float(parts[1].strip()) if len(parts) > 1 else min_target
        except (ValueError, IndexError):
            print(f"Warning: Invalid target format '{args.target}', using defaults")
            min_target = args.min_pct
            max_target = args.max_pct

    # Determine if within band
    in_band = min_target <= catcher_share_pct <= max_target

    print(f"=== CATCHER SHARE AUDIT (Top {args.topn}) ===")
    print(f"Total players: {total_players}")
    print(f"Top {args.topn} examined: {total_top_n}")
    print(
        f"Catchers in Top {args.topn}: {catcher_count}/{total_top_n} ({catcher_share_pct:.1f}%)"
    )
    print(f"Target band: {min_target:.1f}% - {max_target:.1f}%")
    print()

    status_icon = "✓" if in_band else "✗"
    print(f"{status_icon} STATUS: {'PASS' if in_band else 'FAIL'}")

    if catchers_in_top_n:
        print(f"\n=== CATCHERS IN TOP {args.topn} (by TVP_CURRENT) ===")
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
                if last_season_adjustment:
                    adjustments = (
                        f"play_time={last_season_adjustment.get('playing_time_factor', 1.0):.2f}, "
                        f"decline={last_season_adjustment.get('catcher_aging_mult', 1.0):.2f}, "
                        f"pos_chg={last_season_adjustment.get('position_change_prob', 0.0):.2f}"
                    )
                else:
                    adjustments = "N/A"
            else:
                adjustments = "N/A"

            print(
                f"{rank:<6} {name:<25} {mlb_id if isinstance(mlb_id, int) else 'N/A':<10} {tvp:<15.3f} {adjustments}"
            )

    # Print additional Top N stats if multiple requested
    if args.topn not in [25, 50]:
        print(f"\n=== ADDITIONAL TOP N STATS ===")
        for n in [25, 50, 100]:
            if n <= args.topn:
                top_n_subset = sorted_players[:n]
                catchers_in_subset = [
                    p for p in top_n_subset if projection(p).get("is_catcher") is True
                ]
                pct_subset = (len(catchers_in_subset) / n * 100.0) if n > 0 else 0.0
                print(f"Top {n:3d}: {len(catchers_in_subset)}/{n} ({pct_subset:.1f}%)")

    print()
    print("=== SUMMARY ===")
    print(f"Target band: {min_target:.1f}% - {max_target:.1f}%")
    print(f"Actual: {catcher_share_pct:.1f}%")
    if in_band:
        print(f"Result: Within acceptable bounds ✓")
    else:
        print(f"Result: Outside acceptable bounds ✗")

    if not in_band and args.fail_on_outside_band:
        print(
            f"\n❌ ERROR: Catcher share ({catcher_share_pct:.1f}%) is outside target band ({min_target:.1f}% - {max_target:.1f}%)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
