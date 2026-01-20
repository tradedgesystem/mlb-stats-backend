#!/usr/bin/env python3
"""
Audit catcher TVP haircuts by age to verify stability guardrails.
Computes TVP with and without catcher adjustments for real catchers.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVP_PATH = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_config() -> dict:
    config_path = REPO_ROOT / "backend" / "tvp_mlb_defaults.json"
    with config_path.open("r") as handle:
        return json.load(handle)


def run_compute_mlb_tvp(disable_catcher_adjust: bool) -> dict:
    """
    Run compute_mlb_tvp.py as a subprocess with given flag and return parsed output.
    Uses a temporary output file to capture results.
    """
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        dir=str(REPO_ROOT / "backend" / "output"),
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)

    cmd = [
        sys.executable,
        str(REPO_ROOT / "backend" / "compute_mlb_tvp.py"),
        "--no-position-refresh",
        "--output",
        str(tmp_path),
    ]
    if disable_catcher_adjust:
        cmd.append("--disable-catcher-adjust")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        print(f"ERROR: compute_mlb_tvp failed with return code {result.returncode}")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        raise SystemExit("Failed to run compute_mlb_tvp")

    try:
        with tmp_path.open("r") as f:
            data = json.load(f)
        return data
    finally:
        # Clean up temporary file
        if tmp_path.exists():
            tmp_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit catcher TVP haircuts by age to verify guardrails."
    )
    parser.add_argument(
        "--tvp",
        type=Path,
        help="Path to tvp_mlb_*.json (default: compute from scratch).",
    )
    parser.add_argument(
        "--use-existing-output",
        action="store_true",
        help="Use existing TVP output file instead of recomputing (for faster runs).",
    )
    args = parser.parse_args()

    config = load_config()
    snapshot_year = config.get("snapshot_year", 2026)

    if args.tvp and args.use_existing_output:
        tvp_path = args.tvp
        if not tvp_path.exists():
            raise SystemExit(f"TVP file not found: {tvp_path}")
        data = load_json(tvp_path)

        print("Using existing TVP output file")
        print("Note: This only computes haircuts from stored fwar_before_catcher_risk.")
        print("      To recompute from scratch, run without --use-existing-output.")
        print()

        players = data.get("players", [])
        # In legacy mode, don't set tvp_without_catcher_adjust field
        for player in players:
            player["tvp_without_catcher_adjust"] = None
    else:
        print("Computing TVP with and without catcher adjustments...")
        print("(this may take a minute or two)")
        print()

        data_with_adjust = run_compute_mlb_tvp(disable_catcher_adjust=False)
        data_without_adjust = run_compute_mlb_tvp(disable_catcher_adjust=True)

        players = data_with_adjust.get("players", [])
        players_without_adjust_map = {
            p["mlb_id"]: p
            for p in data_without_adjust.get("players", [])
            if p.get("mlb_id")
        }

        for player in players:
            mlb_id = player.get("mlb_id")
            if mlb_id in players_without_adjust_map:
                player_without = players_without_adjust_map[mlb_id]
                player["tvp_without_catcher_adjust"] = player_without.get("tvp_current")
            else:
                player["tvp_without_catcher_adjust"] = None

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
    print()

    # Group by age for analysis
    by_age: dict[int, list[dict]] = {}
    for player in catchers:
        age = player.get("age")
        if age is not None:
            by_age.setdefault(age, []).append(player)

    # Check guardrails and collect violations
    violations = []

    # Additional check: verify no capped age group violates cap
    cap_violations = []

    for age, age_group in sorted(by_age.items()):
        print(f"--- Age {age} ({len(age_group)} players) ---")

        for player in age_group:
            name = player.get("player_name")
            mlb_id = player.get("mlb_id")
            tvp_with_adj = player.get("tvp_current")
            guardrail_max = None

            # Get baseline from computed results or stored fwar_before_catcher_risk
            if player.get("tvp_without_catcher_adjust") is not None:
                # Using recomputed baseline
                tvp_without_adj = player["tvp_without_catcher_adjust"]
            else:
                # Using stored baseline (legacy mode)
                proj = projection(player)
                fwar_before = proj.get("fwar_before_catcher_risk", {})
                fwar_after = proj.get("projected_fwar_by_season", {})

                # Approximate TVP ratio from WAR ratio
                total_before = sum(fwar_before.values()) if fwar_before else 0.0
                total_after = sum(fwar_after.values()) if fwar_after else 0.0

                if total_before > 0 and total_after > 0:
                    ratio = total_after / total_before
                    tvp_without_adj = tvp_with_adj / ratio
                else:
                    tvp_without_adj = tvp_with_adj

            if tvp_without_adj is None or tvp_without_adj == 0:
                print(
                    f"  {name:<25} (mlb_id={mlb_id}): Skipped (no baseline data or zero TVP)"
                )
                continue

            # Check if cap was applied and verify it's within guardrail
            proj = projection(player)
            haircut_cap_applied = proj.get("haircut_cap_applied", False)
            haircut_capped_pct = proj.get("haircut_capped_pct")
            
            if haircut_cap_applied and haircut_capped_pct is not None:
                # Cap was applied - verify it's within guardrail
                if guardrail_max is not None and haircut_capped_pct > guardrail_max:
                    cap_violations.append(
                        {
                            "player": name,
                            "mlb_id": mlb_id,
                            "age": age,
                            "haircut_capped_pct": haircut_capped_pct,
                            "guardrail_max": guardrail_max,
                            "message": f"Cap applied but capped value {haircut_capped_pct}% exceeds guardrail {guardrail_max}%",
                        }
                    )

            # Calculate raw haircut from baseline (only when baseline is available)
            if tvp_without_adj is not None and tvp_without_adj > 0:
                haircut_pct = (
                    (1.0 - tvp_with_adj / tvp_without_adj) * 100.0
                else 0.0
            else:
                # No baseline available - can't compute raw haircut
                haircut_pct = None

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
            else:
                # age >= 30: no cap, just report
                guardrail_max = None

            print(
                f"  {name:<25} (mlb_id={mlb_id}) {guardrail_status} "
                f"TVP={tvp_with_adj:7.2f}→{tvp_without_adj:7.2f} "
                f"haircut={haircut_pct:5.1f}%"
                + (f" (max={guardrail_max:5.1f}%)" if guardrail_max else "")
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

    if cap_violations:
        print(f"\n=== CAP VIOLATIONS ({len(cap_violations)}) ===")
        for v in cap_violations:
            print(
                f"  {v['player']:<25} (mlb_id={v['mlb_id']}) "
                f"age={v['age']} haircut_capped={v['haircut_capped_pct']:.1f}% "
                f"cap_value={v['guardrail_max']:.1f}% - {v['message']}"
            )
        print(
            f"\n❌ FAILED: {len(cap_violations)} cap violations found (capped values exceed guardrails)"
        )
        sys.exit(1)

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
        tvp_without_adj = player.get("tvp_without_catcher_adjust")

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
        else:
            # Fallback to stored data
            proj = projection(player)
            fwar_before = proj.get("fwar_before_catcher_risk", {})
            fwar_after = proj.get("projected_fwar_by_season", {})

            total_before = sum(fwar_before.values()) if fwar_before else 0.0
            total_after = sum(fwar_after.values()) if fwar_after else 0.0

            if total_before > 0 and total_after > 0:
                haircut = (1.0 - total_after / total_before) * 100.0
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
