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
    with TVP_OUTPUT_PATH.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data.get("players", [])


def classify_early_sample(
    is_pitcher: bool,
    pa_value: float | None,
    ip_value: float | None,
    age: int | None,
    seasons_used: int | None,
) -> bool:
    if age is None or age > 26:
        return False
    if is_pitcher:
        return ip_value is not None and ip_value < 80
    return pa_value is not None and pa_value < 300


def reason_not_eligible(
    is_pitcher: bool,
    pa_value: float | None,
    ip_value: float | None,
    age: int | None,
    seasons_used: int | None,
    early_sample: bool,
) -> str:
    if age is None or age > 26:
        return "other"
    if pa_value is not None and pa_value >= 300:
        return "pa_ge_300"
    if ip_value is not None and ip_value >= 80:
        return "ip_ge_80"
    if pa_value is None and ip_value is None:
        if age is None or seasons_used is None:
            return "missing_pa_ip_and_fallback_failed"
        if not (age <= 25 and seasons_used <= 1):
            return "missing_pa_ip_and_fallback_failed"
    if early_sample:
        return "other"
    return "other"


def build_explain_row(player: dict) -> dict:
    raw = player.get("raw_components") or {}
    transition = raw.get("rookie_transition") or {}
    war_inputs = raw.get("war_inputs") or {}
    is_pitcher = war_inputs.get("is_pitcher") is True
    pa_value = transition.get("pa")
    ip_value = transition.get("ip")
    age = player.get("age")
    seasons_used = war_inputs.get("war_history_seasons_used")
    early_sample = classify_early_sample(
        is_pitcher, pa_value, ip_value, age, seasons_used
    )
    reason = reason_not_eligible(
        is_pitcher, pa_value, ip_value, age, seasons_used, early_sample
    )
    has_anchor = (
        transition.get("fv_value") is not None
        and transition.get("prospect_tvp") is not None
    )

    return {
        "player_name": player.get("player_name"),
        "mlb_id": player.get("mlb_id"),
        "tvp_mlb": player.get("tvp_mlb"),
        "tvp_current": player.get("tvp_current"),
        "rookie_transition_applied": transition.get("applied") is True,
        "early_sample_eligible": early_sample,
        "reason_not_eligible": reason,
        "has_anchor": has_anchor,
        "pa": pa_value,
        "ip": ip_value,
        "age": age,
        "seasons_used": seasons_used,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explain rookie transition eligibility for Top 25 by tvp_current."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=25,
        help="Number of top players to explain (default: 25).",
    )
    args = parser.parse_args()

    refresh_tvp()
    players = load_players()
    valid_players = [
        p for p in players if p.get("tvp_current") is not None and p.get("tvp_mlb") is not None
    ]
    sorted_players = sorted(valid_players, key=lambda x: x["tvp_current"], reverse=True)
    top_players = sorted_players[: args.count]

    header = (
        "player_name, mlb_id, tvp_mlb, tvp_current, rookie_transition_applied, "
        "early_sample_eligible, reason_not_eligible, has_anchor, pa, ip, age, seasons_used"
    )
    print(header)
    for player in top_players:
        row = build_explain_row(player)
        print(
            f"{row['player_name']}, {row['mlb_id']}, {row['tvp_mlb']:.3f}, "
            f"{row['tvp_current']:.3f}, {row['rookie_transition_applied']}, "
            f"{row['early_sample_eligible']}, {row['reason_not_eligible']}, "
            f"{row['has_anchor']}, {row['pa']}, {row['ip']}, {row['age']}, "
            f"{row['seasons_used']}"
        )


if __name__ == "__main__":
    main()
