#!/usr/bin/env python3
"""
Audit catcher share in Top N TVP_CURRENT list.
Tests whether catcher representation stays within acceptable bounds.
Supports multiple Top N values and band-based target ranges.
"""

from __future__ import annotations

import argparse
import json
import re
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


def parse_targets(target_str: str) -> dict[int, dict]:
    """
    Parse targets string like "25:1-2,50:0.05-0.07,100:0.04-0.08"

    Returns dict mapping TopN to target spec:
    - Count range: {"min_count": 1, "max_count": 2, "type": "count"}
    - Percentage band: {"min_pct": 5.0, "max_pct": 7.0, "type": "pct"}
    """
    targets = {}
    for pair in target_str.split(","):
        pair = pair.strip()
        if not pair:
            continue

        try:
            topn_str, range_str = pair.split(":")
            topn = int(topn_str.strip())
        except ValueError:
            raise ValueError(
                f"Invalid target pair format: '{pair}'. Expected 'TopN:range'"
            )

        # Check if it's a count range (integers) or pct band (decimals)
        if re.match(r"^\d+-\d+$", range_str):
            # Count range: e.g., "1-2"
            min_count, max_count = range_str.split("-")
            targets[topn] = {
                "min_count": int(min_count.strip()),
                "max_count": int(max_count.strip()),
                "type": "count",
            }
        elif re.match(r"^\d+\.?\d*-\d+\.?\d*$", range_str):
            # Percentage band: e.g., "0.05-0.07"
            min_pct, max_pct = range_str.split("-")
            targets[topn] = {
                "min_pct": float(min_pct.strip()) * 100.0,
                "max_pct": float(max_pct.strip()) * 100.0,
                "type": "pct",
            }
        else:
            raise ValueError(
                f"Invalid range format: '{range_str}'. Use '1-2' for count or '0.05-0.07' for percentage"
            )

    return targets


def check_target(
    catcher_count: int, total_top_n: int, target: dict
) -> tuple[bool, str]:
    """Check if catcher count/share is within target. Returns (in_band, reason)."""
    if target["type"] == "count":
        min_count = target["min_count"]
        max_count = target["max_count"]
        in_band = min_count <= catcher_count <= max_count
        reason = f"{catcher_count} catchers (target: {min_count}-{max_count})"
    else:
        min_pct = target["min_pct"]
        max_pct = target["max_pct"]
        pct = (catcher_count / total_top_n * 100.0) if total_top_n > 0 else 0.0
        in_band = min_pct <= pct <= max_pct
        reason = f"{pct:.1f}% catchers (target: {min_pct:.1f}%-{max_pct:.1f}%)"

    return in_band, reason


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
        "--targets",
        type=str,
        default=None,
        help="Targets per TopN, e.g., '25:1-2,50:0.05-0.07,100:0.04-0.08'. "
        "Count range: '1-2' means 1-2 catchers. Percentage band: '0.05-0.07' means 5-7%.",
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

    # Parse targets if provided
    targets = None
    if args.targets:
        try:
            targets = parse_targets(args.targets)
        except ValueError as e:
            raise SystemExit(f"Error parsing targets: {e}")

    # Fall back to legacy behavior if no targets provided
    top_n = sorted_players[: args.topn]
    catchers_in_top_n = [p for p in top_n if projection(p).get("is_catcher") is True]
    non_catchers_in_top_n = [
        p for p in top_n if projection(p).get("is_catcher") is False
    ]

    catcher_count = len(catchers_in_top_n)
    total_top_n = len(top_n)
    catcher_share_pct = (catcher_count / total_top_n * 100.0) if total_top_n else 0.0

    # Determine targets and check bands
    all_results = []
    overall_in_band = True

    if targets:
        # Check each TopN target
        for topn_value, target in sorted(targets.items()):
            topn_subset = sorted_players[:topn_value]
            catchers_in_subset = [
                p for p in topn_subset if projection(p).get("is_catcher") is True
            ]
            catcher_count_subset = len(catchers_in_subset)
            in_band, reason = check_target(catcher_count_subset, topn_value, target)
            all_results.append(
                {
                    "topn": topn_value,
                    "catcher_count": catcher_count_subset,
                    "total": topn_value,
                    "in_band": in_band,
                    "reason": reason,
                }
            )
            if not in_band:
                overall_in_band = False
    else:
        # Legacy behavior: use single TopN
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

        in_band = min_target <= catcher_share_pct <= max_target
        overall_in_band = in_band
        all_results.append(
            {
                "topn": args.topn,
                "catcher_count": catcher_count,
                "total": total_top_n,
                "in_band": in_band,
                "reason": f"{catcher_share_pct:.1f}% catchers (target: {min_target:.1f}%-{max_target:.1f}%)",
            }
        )

    # Print results
    if targets:
        print(f"=== CATCHER SHARE AUDIT (Multiple Top N) ===")
        print(f"Total players: {total_players}")
        print(f"Valid players: {len(valid_players)}")
        print()

        for result in all_results:
            status_icon = "✓" if result["in_band"] else "✗"
            status_str = "PASS" if result["in_band"] else "FAIL"
            print(
                f"Top {result['topn']:3d}: {result['catcher_count']}/{result['total']} - {result['reason']} {status_icon} {status_str}"
            )

        print()
        status_icon = "✓" if overall_in_band else "✗"
        print(f"{status_icon} STATUS: {'PASS' if overall_in_band else 'FAIL'}")

        # Show catchers in Top 25 and Top 50 if needed
        top_25_players = sorted_players[:25]
        catchers_top_25 = [
            p for p in top_25_players if projection(p).get("is_catcher") is True
        ]

        top_50_players = sorted_players[:50]
        catchers_top_50 = [
            p for p in top_50_players if projection(p).get("is_catcher") is True
        ]

        if catchers_top_25:
            print(f"\n=== CATCHERS IN TOP 25 (by TVP_CURRENT) ===")
            print(
                f"{'Rank':<6} {'Player':<25} {'MLB ID':<10} {'TVP_CURRENT':<15} {'Adjustments'}"
            )
            print("-" * 80)

            for player in catchers_top_25:
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

        # Show catchers in Top 50 if different from Top 25
        catchers_top_50_unique = [
            p for p in catchers_top_50 if p not in catchers_top_25
        ]
        if catchers_top_50_unique:
            print(f"\n=== CATCHERS IN TOP 50 (not in Top 25) ===")
            print(
                f"{'Rank':<6} {'Player':<25} {'MLB ID':<10} {'TVP_CURRENT':<15} {'Adjustments'}"
            )
            print("-" * 80)

            for player in catchers_top_50_unique:
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
    else:
        # Legacy output
        print(f"=== CATCHER SHARE AUDIT (Top {args.topn}) ===")
        print(f"Total players: {total_players}")
        print(f"Top {args.topn} examined: {total_top_n}")
        print(
            f"Catchers in Top {args.topn}: {catcher_count}/{total_top_n} ({catcher_share_pct:.1f}%)"
        )

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
        overall_in_band = in_band

        print(f"Target band: {min_target:.1f}% - {max_target:.1f}%")
        print()

        status_icon = "✓" if overall_in_band else "✗"
        print(f"{status_icon} STATUS: {'PASS' if overall_in_band else 'FAIL'}")

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
                        p
                        for p in top_n_subset
                        if projection(p).get("is_catcher") is True
                    ]
                    pct_subset = (len(catchers_in_subset) / n * 100.0) if n > 0 else 0.0
                    print(
                        f"Top {n:3d}: {len(catchers_in_subset)}/{n} ({pct_subset:.1f}%)"
                    )

    print()
    print("=== SUMMARY ===")
    for result in all_results:
        status_str = "✓" if result["in_band"] else "✗"
        print(f"{status_str} Top {result['topn']:3d}: {result['reason']}")

    print(f"\nOverall: {'PASS' if overall_in_band else 'FAIL'}")

    if not overall_in_band and args.fail_on_outside_band:
        failed_results = [r for r in all_results if not r["in_band"]]
        for result in failed_results:
            print(f"\n❌ ERROR: Top {result['topn']} - {result['reason']} - FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
