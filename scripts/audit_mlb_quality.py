#!/usr/bin/env python3
"""
Audit MLB TVP quality across all players and report suspicious cases.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from compute_mlb_tvp import (
    compute_player_tvp,
    load_players,
    load_war_history,
    parse_weights,
)
from tvp_engine import load_config

REPO_ROOT = Path(__file__).resolve().parents[1]


def compute_suspicion_score(flags: dict[str, Any]) -> int:
    """
    Compute suspicion score from quality flags.
    Exact scoring per specification:
    +10 if salary_below_min_detected
    +8 if guaranteed_includes_option_year
    +6 if option_year_mismatch
    +4 if control_fallback_used
    +3 if war_history_partial
    +2 if projected_war_hits_cap
    +1 if projected_war_years >= 8
    """
    score = 0
    if flags.get("salary_below_min_detected"):
        score += 10
    if flags.get("guaranteed_includes_option_year"):
        score += 8
    if flags.get("option_year_mismatch"):
        score += 6
    if flags.get("control_fallback_used"):
        score += 4
    if flags.get("war_history_partial"):
        score += 3
    if flags.get("projected_war_hits_cap"):
        score += 2
    if flags.get("projected_war_years", 0) >= 8:
        score += 1
    return score


def format_score_breakdown(flags: dict[str, Any], score: int) -> str:
    """Format suspicion score breakdown with only triggered flags."""
    parts = []
    if flags.get("salary_below_min_detected"):
        parts.append("10 salary_below_min")
    if flags.get("guaranteed_includes_option_year"):
        parts.append("8 guaranteed_includes_option")
    if flags.get("option_year_mismatch"):
        parts.append("6 option_year_mismatch")
    if flags.get("control_fallback_used"):
        parts.append("4 control_fallback")
    if flags.get("war_history_partial"):
        parts.append("3 war_history_partial")
    if flags.get("projected_war_hits_cap"):
        parts.append("2 projected_war_hits_cap")
    if flags.get("projected_war_years", 0) >= 8:
        parts.append("1 projected_war_years>=8")

    if parts:
        return f"score={score} ({' + '.join(parts)})"
    else:
        return f"score=0"


def refresh_tvp() -> None:
    """Run MLB TVP computation pipeline."""
    compute_tvp_path = REPO_ROOT / "backend" / "compute_mlb_tvp.py"
    result = subprocess.run(
        ["python3", str(compute_tvp_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error running TVP computation: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit MLB TVP quality and report suspicious cases."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to precomputed TVP JSON (default: recompute from scratch).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of players to audit (default: all).",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip TVP recomputation (requires --input).",
    )
    args = parser.parse_args()

    # Load data
    if args.input:
        # Load precomputed TVP data
        if not args.input.exists():
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        import json

        with args.input.open("r") as f:
            data = json.load(f)
        players = data.get("players", [])
        print(f"Loaded {len(players)} players from {args.input}")
    else:
        # Recompute from scratch
        if args.no_refresh:
            print("Error: --no-refresh requires --input", file=sys.stderr)
            sys.exit(1)

        print("Recomputing TVP data from scratch...")
        refresh_tvp()

        # Load recomputed data
        import json

        tvp_output_path = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"
        with tvp_output_path.open("r") as f:
            data = json.load(f)
        players = data.get("players", [])
        print(f"Loaded {len(players)} recomputed players")

    # Apply limit
    if args.limit:
        players = players[: args.limit]
        print(f"Limited to {len(players)} players")

    # Compute suspicion scores and sort
    players_with_scores = []
    for player in players:
        flags = player.get("raw_components", {}).get("quality_flags", {})
        tvp_current = player.get("tvp_current")
        if flags is None:
            continue

        score = compute_suspicion_score(flags)
        players_with_scores.append(
            {
                "player_name": player.get("player_name"),
                "mlb_id": player.get("mlb_id"),
                "tvp_current": tvp_current,
                "suspicion_score": score,
                "flags": flags,
                "score_breakdown": format_score_breakdown(flags, score),
            }
        )

    # Sort by TVP current (top 50)
    top_by_tvp = sorted(
        players_with_scores, key=lambda x: x["tvp_current"] or 0, reverse=True
    )[:50]

    # Sort by suspicion score (top 50)
    top_suspicious = sorted(
        players_with_scores, key=lambda x: x["suspicion_score"], reverse=True
    )[:50]

    # Output: Top 50 by TVP
    print("\n" + "=" * 80)
    print("TOP 50 PLAYERS BY TVP")
    print("=" * 80)
    print("rank, player_name, mlb_id, tvp_current")
    for rank, player in enumerate(top_by_tvp, 1):
        tvp_value = player["tvp_current"]
        tvp_str = f"{tvp_value:.3f}" if tvp_value is not None else "N/A"
        print(f"{rank}, {player['player_name']}, {player['mlb_id']}, {tvp_str}")

    # Output: Top 50 most suspicious
    print("\n" + "=" * 80)
    print("TOP 50 MOST SUSPICIOUS PLAYERS")
    print("=" * 80)
    print("rank, player_name, mlb_id, tvp_current, suspicion_score, flags")
    for rank, player in enumerate(top_suspicious, 1):
        tvp_value = player["tvp_current"]
        tvp_str = f"{tvp_value:.3f}" if tvp_value is not None else "N/A"
        print(
            f"{rank}, {player['player_name']}, {player['mlb_id']}, {tvp_str}, {player['suspicion_score']}, {player['score_breakdown']}"
        )


if __name__ == "__main__":
    main()
