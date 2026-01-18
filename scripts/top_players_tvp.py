#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get top N most valuable players by TVP."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=25,
        help="Number of top players to return (default: 25).",
    )
    args = parser.parse_args()

    refresh_tvp()
    players = load_players()

    valid_players = [p for p in players if p.get("tvp_mlb") is not None]
    sorted_players = sorted(valid_players, key=lambda x: x["tvp_mlb"], reverse=True)
    top_players = sorted_players[: args.count]

    for rank, player in enumerate(top_players, 1):
        player_name = player.get("player_name")
        mlb_id = player.get("mlb_id")
        tvp_mlb = player.get("tvp_mlb")
        print(f"{rank}, {player_name}, {mlb_id}, {tvp_mlb:.3f}")


if __name__ == "__main__":
    main()
