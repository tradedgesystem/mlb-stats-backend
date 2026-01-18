#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TVP_OUTPUT_PATH = REPO_ROOT / "backend" / "output" / "tvp_mlb_2026.json"


def refresh_tvp() -> None:
    compute_tvp_path = REPO_ROOT / "backend" / "compute_mlb_tvp.py"
    subprocess.run(["python3", str(compute_tvp_path)], check=True, cwd=REPO_ROOT)


def load_players() -> list[dict]:
    with TVP_OUTPUT_PATH.open("r") as f:
        data = json.load(f)
    return data.get("players", [])


def find_player(
    players: list[dict], player_name: str, mlb_id: int | None
) -> dict | None:
    if mlb_id is not None:
        for player in players:
            if player.get("mlb_id") == mlb_id:
                return player
        return None

    exact_matches = [p for p in players if p.get("player_name") == player_name]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        ids = [p.get("mlb_id") for p in exact_matches]
        print(
            f"Ambiguous name '{player_name}' (mlb_ids={ids}). Use --mlb-id.",
            file=sys.stderr,
        )
        return None

    case_insensitive_matches = [
        p for p in players if p.get("player_name", "").lower() == player_name.lower()
    ]
    if len(case_insensitive_matches) == 1:
        return case_insensitive_matches[0]
    if len(case_insensitive_matches) > 1:
        ids = [p.get("mlb_id") for p in case_insensitive_matches]
        print(
            f"Ambiguous name '{player_name}' (mlb_ids={ids}). Use --mlb-id.",
            file=sys.stderr,
        )
        return None

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Get MLB TVP for a specific player.")
    parser.add_argument("--player", required=True, help="Player name to look up.")
    parser.add_argument(
        "--mlb-id", type=int, help="Optional MLB ID for disambiguation."
    )
    args = parser.parse_args()

    refresh_tvp()
    players = load_players()

    player = find_player(players, args.player, args.mlb_id)
    if player is None:
        print("no match")
        sys.exit(0)

    tvp_mlb = player.get("tvp_mlb")
    mlb_id = player.get("mlb_id")
    player_name = player.get("player_name")

    if tvp_mlb is None:
        print("no match")
        sys.exit(0)

    print(f"{player_name}, {mlb_id}, {tvp_mlb:.3f}")


if __name__ == "__main__":
    main()
