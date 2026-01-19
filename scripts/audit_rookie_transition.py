from __future__ import annotations

import json
import statistics
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_latest_tvp() -> dict:
    config_path = REPO_ROOT / "backend" / "tvp_config.json"
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    snapshot_year = config.get("snapshot_year", 2026)
    output_path = REPO_ROOT / "backend" / "output" / f"tvp_mlb_{snapshot_year}.json"
    if not output_path.exists():
        subprocess.run(
            ["python3", "backend/compute_mlb_tvp.py"],
            check=True,
            cwd=REPO_ROOT,
        )
    with output_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def format_row(row: list) -> str:
    return ", ".join(str(item) for item in row)


def main() -> None:
    data = load_latest_tvp()
    players = data.get("players", [])
    total_players = len(players)
    applied = []
    not_applied = []
    alphas = []

    for player in players:
        raw = player.get("raw_components") or {}
        transition = raw.get("rookie_transition") or {}
        applied_flag = transition.get("applied") is True
        tvp_mlb_base = (
            (raw.get("economics_pv") or {}).get("tvp_mlb_base")
            or player.get("tvp_mlb")
            or 0.0
        )
        tvp_current = player.get("tvp_current") or player.get("tvp_mlb") or 0.0
        delta = tvp_current - tvp_mlb_base
        if applied_flag:
            alpha = transition.get("alpha")
            if isinstance(alpha, (int, float)):
                alphas.append(float(alpha))
            applied.append(
                {
                    "player_name": player.get("player_name"),
                    "mlb_id": player.get("mlb_id"),
                    "tvp_mlb_base": tvp_mlb_base,
                    "tvp_current": tvp_current,
                    "delta": delta,
                    "alpha": transition.get("alpha"),
                    "pa": transition.get("pa"),
                    "ip": transition.get("ip"),
                    "age": player.get("age"),
                    "seasons_used": (raw.get("war_inputs") or {}).get(
                        "war_history_seasons_used"
                    ),
                }
            )
        else:
            is_pitcher = (raw.get("war_inputs") or {}).get("is_pitcher")
            pa_value = transition.get("pa")
            ip_value = transition.get("ip")
            age = player.get("age")
            seasons_used = (raw.get("war_inputs") or {}).get(
                "war_history_seasons_used"
            )
            has_anchor = transition.get("prospect_tvp") is not None or transition.get(
                "fv_value"
            ) is not None

            reason = transition.get("reason_not_applied")
            if reason is None:
                if not has_anchor:
                    reason = "missing_anchor"
                else:
                    used_sample_gate = pa_value is not None or ip_value is not None
                    if used_sample_gate:
                        if is_pitcher:
                            if ip_value is None:
                                reason = "missing_pa_ip"
                            elif ip_value < 500:
                                reason = "gate_failed"
                            else:
                                reason = "not_early_sample"
                        else:
                            if pa_value is None:
                                reason = "missing_pa_ip"
                            elif pa_value < 2000:
                                reason = "gate_failed"
                            else:
                                reason = "not_early_sample"
                    else:
                        if age is None or seasons_used is None:
                            reason = "missing_pa_ip"
                        elif age <= 25 and seasons_used <= 2:
                            reason = "gate_failed"
                        else:
                            reason = "not_early_sample"

            not_applied.append(
                {
                    "player_name": player.get("player_name"),
                    "mlb_id": player.get("mlb_id"),
                    "tvp_mlb_base": tvp_mlb_base,
                    "tvp_current": tvp_current,
                    "delta": delta,
                    "alpha": transition.get("alpha"),
                    "reason": reason,
                    "pa": pa_value,
                    "ip": ip_value,
                    "age": age,
                    "seasons_used": seasons_used,
                }
            )

    applied_count = len(applied)
    pct_applied = (applied_count / total_players * 100.0) if total_players else 0.0

    print(format_row(["total_players", total_players]))
    print(format_row(["count_rookie_transition_applied", applied_count]))
    print(format_row(["pct_applied", f"{pct_applied:.2f}%"]))
    if alphas:
        print(
            format_row(
                [
                    "alpha_distribution",
                    f"min={min(alphas):.4f}",
                    f"median={statistics.median(alphas):.4f}",
                    f"max={max(alphas):.4f}",
                ]
            )
        )
    else:
        print("alpha_distribution, none")

    print("\nTop 25 by TVP delta (applied)")
    print(
        "rank, player_name, mlb_id, delta, tvp_mlb_base, tvp_current, alpha, pa, ip, age, seasons_used"
    )
    for idx, row in enumerate(sorted(applied, key=lambda x: x["delta"], reverse=True)[:25], 1):
        print(
            format_row(
                [
                    idx,
                    row["player_name"],
                    row["mlb_id"],
                    f"{row['delta']:.3f}",
                    f"{row['tvp_mlb_base']:.3f}",
                    f"{row['tvp_current']:.3f}",
                    row["alpha"],
                    row["pa"],
                    row["ip"],
                    row["age"],
                    row["seasons_used"],
                ]
            )
        )

    print("\nTop 25 should-have-applied-but-didn’t")
    print(
        "rank, player_name, mlb_id, reason, delta, tvp_mlb_base, tvp_current, alpha, pa, ip, age, seasons_used"
    )
    for idx, row in enumerate(
        sorted(not_applied, key=lambda x: x["tvp_mlb_base"], reverse=True)[:25], 1
    ):
        print(
            format_row(
                [
                    idx,
                    row["player_name"],
                    row["mlb_id"],
                    row["reason"],
                    f"{row['delta']:.3f}",
                    f"{row['tvp_mlb_base']:.3f}",
                    f"{row['tvp_current']:.3f}",
                    row["alpha"],
                    row["pa"],
                    row["ip"],
                    row["age"],
                    row["seasons_used"],
                ]
            )
        )


if __name__ == "__main__":
    main()
