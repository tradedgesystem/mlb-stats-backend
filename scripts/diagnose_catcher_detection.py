#!/usr/bin/env python3
"""
Diagnostic script to check catcher detection in TVP output.
Run with: python3 scripts/diagnose_catcher_detection.py
"""
import json
from pathlib import Path


TARGET_PLAYERS = [
    ("Cal Raleigh", 663728),
    ("Will Smith", 669257),
    ("Geraldo Perdomo", 672695),
]


def load_json(path: Path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def find_first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    tvp_path = find_first_existing(
        [
            repo_root / "backend" / "output" / "tvp_mlb_2026.json",
            repo_root / "backend" / "output" / "tvp_mlb_2025.json",
            repo_root / "backend" / "output" / "tvp_mlb.json",
        ]
    )
    if not tvp_path:
        print("No TVP output found under backend/output.")
        return

    tvp_data = load_json(tvp_path) or {}
    players = tvp_data.get("players", [])

    print(f"TVP source: {tvp_path}")
    print("=== CATCHER DETECTION DIAGNOSTIC ===")
    for name, mlb_id in TARGET_PLAYERS:
        player = next((p for p in players if p.get("mlb_id") == mlb_id), None)
        print(f"\nPlayer: {name} ({mlb_id})")
        if not player:
            print("  Not found in TVP output.")
            continue

        projection = player.get("raw_components", {}).get("projection", {})
        print(f"  position: {projection.get('position')}")
        print(f"  position_source: {projection.get('position_source')}")
        print(f"  is_catcher: {projection.get('is_catcher')}")
        print(f"  catcher_war_mult: {projection.get('catcher_war_mult')}")
        print(
            "  catcher_war_mult_applied: "
            f"{projection.get('catcher_war_mult_applied')}"
        )


if __name__ == "__main__":
    main()
