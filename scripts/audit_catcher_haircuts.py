#!/usr/bin/env python3
"""
Audit catcher TVP haircuts by age to verify stability guardrails.
Computes TVP with and without catcher adjustments for real catchers.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import tempfile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVP_PATH = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_config() -> dict:
    config_path = REPO_ROOT / "backend" / "tvp_mlb_defaults.json"
    with config_path.open("r") as handle:
        return json.load(handle)


def compute_tvp_without_catcher_adjust(
    player: dict, config: dict, snapshot_year: int
) -> float | None:
    """
    Compute TVP for a catcher without catcher adjustments by temporarily
    disabling catcher-specific adjustments.

    This is a simplified approximation - it calculates the TVP reduction
    that would occur from catcher adjustments.
    """
    tvp_current = player.get("tvp_current")
    if tvp_current is None:
        return None

    proj = player.get("raw_components", {}).get("projection", {})
    catcher_risk_adjustments = proj.get("catcher_risk_adjustments", {})

    if not catcher_risk_adjustments:
        return tvp_current

    # Calculate total reduction from catcher adjustments
    # Sum up the value lost across all seasons
    total_fwar_without_adj = 0.0
    total_fwar_with_adj = 0.0
    seasons_with_adj = 0

    for season, adjustments in catcher_risk_adjustments.items():
        base_fwar = adjustments.get("base_fwar", 0.0)
        final_fwar = adjustments.get("final_fwar", 0.0)
        total_fwar_without_adj += base_fwar
        total_fwar_with_adj += final_fwar
        seasons_with_adj += 1

    if seasons_with_adj == 0 or total_fwar_without_adj == 0:
        return tvp_current

    # Approximate TVP reduction ratio
    reduction_ratio = total_fwar_with_adj / total_fwar_without_adj

    # TVP without catcher adj = TVP with adj / reduction_ratio
    tvp_without_catcher_adj = tvp_current / reduction_ratio

    return tvp_without_catcher_adj


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit catcher TVP haircuts by age to verify guardrails."
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Path to tvp_mlb_*.json (default: backend/output/tvp_mlb_2026.json).",
    )
    args = parser.parse_args()

    tvp_path = args.tvp or DEFAULT_TVP_PATH
    if not tvp_path.exists():
        raise SystemExit(f"TVP file not found: {tvp_path}")

    config = load_config()
    snapshot_year = config.get("snapshot_year", 2026)
    data = load_json(tvp_path)
    players = data.get("players", [])

    def projection(player: dict) -> dict:
        return player.get("raw_components", {}).get("projection", {})

    # Filter for catchers only
    catchers = [
        p
        for p in players
        if projection(p).get("is_catcher") is True and p.get("tvp_current") is not None
    ]

    if not catchers:
        print("No catchers found in TVP output.")
        return

    print("=== CATCHER TVP HAIRCUT AUDIT ===")
    print(f"Total catchers: {len(catchers)}")

    # Group by age for analysis
    by_age: dict[int, list[dict]] = {}
    for player in catchers:
        age = player.get("age")
        if age is not None:
            by_age.setdefault(age, []).append(player)

    # Check guardrails and collect violations
    violations = []

    for age, age_group in sorted(by_age.items()):
        print(f"\n--- Age {age} ({len(age_group)} players) ---")

    for player in age_group:
        name = player.get("player_name")
        mlb_id = player.get("mlb_id")
        tvp_with_adj = player.get("tvp_current")
        tvp_without_adj = compute_tvp_without_catcher_adjust(
            player, config, snapshot_year
        )

        if tvp_without_adj is None or tvp_without_adj == 0:
            print(
                f"  {name:<25} (mlb_id={mlb_id}): Skipped (no adjustment data or zero TVP)"
            )
            continue

        haircut_pct = (
            (1.0 - tvp_with_adj / tvp_without_adj) * 100.0
            if tvp_without_adj > 0
            else 0.0
        )

        # Apply guardrails
        guardrail_status = "✓"
        guardrail_max = None

        if age <= 26:
            guardrail_max = 25.0
            if haircut_pct > guardrail_max:
                guardrail_status = "✗"
                violations.append(
                    {
                        "player": name,
                        "mlb_id": mlb_id,
                        "age": age,
                        "haircut_pct": haircut_pct,
                        "guardrail_max": guardrail_max,
                    }
                )
        elif 27 <= age <= 29:
            guardrail_max = 35.0
            if haircut_pct > guardrail_max:
                guardrail_status = "✗"
                violations.append(
                    {
                        "player": name,
                        "mlb_id": mlb_id,
                        "age": age,
                        "haircut_pct": haircut_pct,
                        "guardrail_max": guardrail_max,
                    }
                )
        # age >= 30: no cap, just report

        print(
            f"  {name:<25} (mlb_id={mlb_id}) {guardrail_status} "
            f"TVP={tvp_with_adj:7.2f}→{tvp_without_adj:7.2f} "
            f"haircut={haircut_pct:5.1f}%"
            f" (max={guardrail_max:5.1f}%)"
            if guardrail_max
            else ""
        )

    if violations:
        print(f"\n=== VIOLATIONS ({len(violations)}) ===")
        for v in violations:
            print(
                f"  {v['player']:<25} (mlb_id={v['mlb_id']}) "
                f"age={v['age']} haircut={v['haircut_pct']:.1f}% "
                f"max={v['guardrail_max']:.1f}%"
            )
        print(f"\n❌ FAILED: {len(violations)} guardrail violations found")
        sys.exit(1)
    else:
        print("\n✓ PASSED: All catcher haircuts within guardrails")

    # Summary stats
    print("\n=== SUMMARY ===")
    avg_haircut_all = 0.0
    count_all = 0
    avg_haircut_young = 0.0
    count_young = 0
    avg_haircut_prime = 0.0
    count_prime = 0

    for player in catchers:
        tvp_with_adj = player.get("tvp_current", 0.0)
        tvp_without_adj = compute_tvp_without_catcher_adjust(
            player, config, snapshot_year
        )
        if tvp_without_adj and tvp_without_adj > 0:
            haircut = (1.0 - tvp_with_adj / tvp_without_adj) * 100.0
            avg_haircut_all += haircut
            count_all += 1

            age = player.get("age", 0)
            if age <= 26:
                avg_haircut_young += haircut
                count_young += 1
            elif 27 <= age <= 29:
                avg_haircut_prime += haircut
                count_prime += 1

    if count_all > 0:
        print(f"Average haircut (all catchers): {avg_haircut_all / count_all:.1f}%")
    if count_young > 0:
        print(f"Average haircut (age ≤ 26): {avg_haircut_young / count_young:.1f}%")
    if count_prime > 0:
        print(f"Average haircut (age 27-29): {avg_haircut_prime / count_prime:.1f}%")


if __name__ == "__main__":
    main()
