#!/usr/bin/env python3
"""
Diagnostic script to check catcher detection.
Run with: python3 scripts/diagnose_catcher_detection.py
"""

import json
import re
from pathlib import Path


def normalize_name(name: str) -> str:
    name = re.sub(r"\(.*?\)", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-zA-Z\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip().lower()
    return re.sub(r"[^a-z]", "", name)


def main():
    repo_root = Path(__file__).resolve().parents[1]

    # Load TVP output
    tvp_path = repo_root / "backend" / "output" / "tvp_mlb_2026.json"
    with tvp_path.open("r") as f:
        tvp_data = json.load(f)

    # Load players_with_contracts.json
    contracts_path = (
        repo_root / "backend" / "output" / "players_with_contracts_2025.json"
    )
    with contracts_path.open("r") as f:
        contracts_data = json.load(f)

    # Build position lookup by mlb_id
    position_lookup = {}
    for player in contracts_data.get("players", []):
        mlb_id = player.get("mlb_id")
        if mlb_id is not None:
            position_lookup[mlb_id] = {
                "player_name": player.get("player_name"),
                "position": player.get("position"),
                "position_source": "contracts_file",
            }

    # Check specific players
    target_players = [
        ("Cal Raleigh", 663728),
        ("Will Smith", 669257),
        ("Geraldo Perdomo", 672695),
    ]

    print("=== CATCHER DETECTION DIAGNOSTIC ===\n")

    for name, mlb_id in target_players:
        print(f"Player: {name}")
        print(f"  MLB ID: {mlb_id}")

        # Find in TVP output
        tvp_player = None
        for p in tvp_data.get("players", []):
            if p.get("mlb_id") == mlb_id:
                tvp_player = p
                break

        if not tvp_player:
            print(f"  NOT FOUND in TVP output")
            print()
            continue

        # Check TVP flags
        projection = tvp_player.get("raw_components", {}).get("projection", {})
        is_catcher = projection.get("is_catcher")
        catcher_war_mult = projection.get("catcher_war_mult")
        catcher_war_mult_applied = projection.get("catcher_war_mult_applied")

        print(f"  TVP is_catcher: {is_catcher}")
        print(f"  TVP catcher_war_mult: {catcher_war_mult}")
        print(f"  TVP catcher_war_mult_applied: {catcher_war_mult_applied}")

        # Check position source
        pos_info = position_lookup.get(mlb_id, {})
        print(f"  Position from contracts: {pos_info.get('position')}")
        print(f"  Position source: {pos_info.get('position_source')}")

        # Determine expected
        expected_catcher = False
        position = pos_info.get("position")
        if position and "C" in str(position).upper():
            expected_catcher = True

        print(f"  Expected catcher: {expected_catcher}")

        # Compare
        if is_catcher == expected_catcher:
            print(f"  ✓ DETECTION CORRECT")
        else:
            print(
                f"  ✗ DETECTION WRONG! (Expected: {expected_catcher}, Got: {is_catcher})"
            )

        print()


if __name__ == "__main__":
    main()
