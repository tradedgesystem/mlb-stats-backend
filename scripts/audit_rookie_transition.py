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


def has_anchor(transition: dict) -> bool:
    return (
        transition.get("fv_value") is not None
        and transition.get("prospect_tvp") is not None
    )


def is_early_sample_candidate(
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


def collect_anchor_coverage(players: list[dict]) -> tuple[dict[str, dict[str, int]], list[dict]]:
    coverage = {
        "overall": {"eligible": 0, "with_anchor": 0, "missing_anchor": 0},
        "hitters": {"eligible": 0, "with_anchor": 0, "missing_anchor": 0},
        "pitchers": {"eligible": 0, "with_anchor": 0, "missing_anchor": 0},
    }
    anchor_sources: dict[str, dict[str, int]] = {
        "overall": {},
        "hitters": {},
        "pitchers": {},
    }
    missing_anchor = []

    for player in players:
        raw = player.get("raw_components") or {}
        transition = raw.get("rookie_transition") or {}
        war_inputs = raw.get("war_inputs") or {}
        is_pitcher = war_inputs.get("is_pitcher") is True
        pa_value = transition.get("pa")
        ip_value = transition.get("ip")
        age = player.get("age")
        seasons_used = war_inputs.get("war_history_seasons_used")
        early_sample = is_early_sample_candidate(
            is_pitcher, pa_value, ip_value, age, seasons_used
        )
        if not early_sample:
            continue

        anchor_source = transition.get("anchor_source")
        if anchor_source is None and has_anchor(transition):
            anchor_source = "prospect"
        anchor_present = anchor_source is not None
        bucket = "pitchers" if is_pitcher else "hitters"
        for key in ("overall", bucket):
            coverage[key]["eligible"] += 1
            if anchor_present:
                coverage[key]["with_anchor"] += 1
            else:
                coverage[key]["missing_anchor"] += 1
            source_counts = anchor_sources[key]
            source_key = anchor_source or "missing"
            source_counts[source_key] = source_counts.get(source_key, 0) + 1

        if not anchor_present:
            economics = raw.get("economics_pv") or {}
            tvp_mlb_base = economics.get("tvp_mlb_base")
            if not isinstance(tvp_mlb_base, (int, float)):
                tvp_mlb_base = player.get("tvp_mlb") or 0.0
            missing_anchor.append(
                {
                    "player_name": player.get("player_name"),
                    "mlb_id": player.get("mlb_id"),
                    "tvp_mlb_base": float(tvp_mlb_base),
                    "pa": pa_value,
                    "ip": ip_value,
                    "age": age,
                    "seasons_used": seasons_used,
                    "is_pitcher": is_pitcher,
                }
            )

    return coverage, missing_anchor, anchor_sources


def collect_audit_rows(players: list[dict]) -> tuple[list[dict], list[dict], list[float]]:
    applied = []
    missing_candidates = []
    alphas: list[float] = []

    for player in players:
        raw = player.get("raw_components") or {}
        transition = raw.get("rookie_transition") or {}
        applied_flag = transition.get("applied") is True
        tvp_current_pre = transition.get("tvp_current_pre") or player.get("tvp_mlb") or 0.0
        tvp_current_post = transition.get("tvp_current_post") or player.get("tvp_current") or 0.0
        delta = transition.get("delta")
        if not isinstance(delta, (int, float)):
            delta = tvp_current_post - tvp_current_pre
        is_pitcher = (raw.get("war_inputs") or {}).get("is_pitcher") is True
        pa_value = transition.get("pa")
        ip_value = transition.get("ip")
        age = player.get("age")
        seasons_used = (raw.get("war_inputs") or {}).get("war_history_seasons_used")
        early_sample = is_early_sample_candidate(
            is_pitcher, pa_value, ip_value, age, seasons_used
        )

        if applied_flag:
            alpha = transition.get("alpha")
            if isinstance(alpha, (int, float)):
                alphas.append(float(alpha))
            applied.append(
                {
                    "player_name": player.get("player_name"),
                    "mlb_id": player.get("mlb_id"),
                    "tvp_current_pre": tvp_current_pre,
                    "tvp_current_post": tvp_current_post,
                    "delta": delta,
                    "alpha": transition.get("alpha"),
                    "pa": pa_value,
                    "ip": ip_value,
                    "age": age,
                    "seasons_used": seasons_used,
                }
            )
            continue

        reason = transition.get("reason_not_applied")
        if early_sample and reason in {"missing_anchor", "missing_pa_ip"}:
            missing_candidates.append(
                {
                    "player_name": player.get("player_name"),
                    "mlb_id": player.get("mlb_id"),
                    "tvp_current_pre": tvp_current_pre,
                    "tvp_current_post": tvp_current_post,
                    "delta": delta,
                    "alpha": transition.get("alpha"),
                    "reason": reason,
                    "pa": pa_value,
                    "ip": ip_value,
                    "age": age,
                    "seasons_used": seasons_used,
                }
            )

    return applied, missing_candidates, alphas


def main() -> None:
    data = load_latest_tvp()
    players = data.get("players", [])
    total_players = len(players)
    applied, missing_candidates, alphas = collect_audit_rows(players)
    coverage, missing_anchor, anchor_sources = collect_anchor_coverage(players)

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

    for label in ("overall", "hitters", "pitchers"):
        eligible = coverage[label]["eligible"]
        with_anchor = coverage[label]["with_anchor"]
        missing_anchor_count = coverage[label]["missing_anchor"]
        pct_with = (with_anchor / eligible * 100.0) if eligible else 0.0
        pct_missing = (missing_anchor_count / eligible * 100.0) if eligible else 0.0
        print(
            format_row(
                [
                    f"early_sample_anchor_coverage_{label}",
                    f"eligible={eligible}",
                    f"pct_with_anchor={pct_with:.2f}%",
                    f"pct_missing_anchor={pct_missing:.2f}%",
                ]
            )
        )

    for label in ("overall", "hitters", "pitchers"):
        source_counts = anchor_sources[label]
        if not source_counts:
            print(format_row([f"early_sample_anchor_source_{label}", "none"]))
            continue
        parts = []
        for source_key, count in sorted(source_counts.items()):
            pct = (count / coverage[label]["eligible"] * 100.0) if coverage[label]["eligible"] else 0.0
            parts.append(f"{source_key}={count} ({pct:.2f}%)")
        print(format_row([f"early_sample_anchor_source_{label}"] + parts))
    print("\nTop 25 by delta_rookie_transition (applied)")
    print(
        "rank, player_name, mlb_id, delta, tvp_current_pre, tvp_current_post, alpha, pa, ip, age, seasons_used"
    )
    for idx, row in enumerate(
        sorted(applied, key=lambda x: x["delta"], reverse=True)[:25], 1
    ):
        print(
            format_row(
                [
                    idx,
                    row["player_name"],
                    row["mlb_id"],
                    f"{row['delta']:.3f}",
                    f"{row['tvp_current_pre']:.3f}",
                    f"{row['tvp_current_post']:.3f}",
                    row["alpha"],
                    row["pa"],
                    row["ip"],
                    row["age"],
                    row["seasons_used"],
                ]
            )
        )

    print("\nTop 25 missing_anchor/missing_pa_ip among early-sample players")
    print(
        "rank, player_name, mlb_id, reason, delta, tvp_current_pre, tvp_current_post, alpha, pa, ip, age, seasons_used"
    )
    for idx, row in enumerate(
        sorted(missing_candidates, key=lambda x: x["tvp_current_pre"], reverse=True)[:25],
        1,
    ):
        print(
            format_row(
                [
                    idx,
                    row["player_name"],
                    row["mlb_id"],
                    row["reason"],
                    f"{row['delta']:.3f}",
                    f"{row['tvp_current_pre']:.3f}",
                    f"{row['tvp_current_post']:.3f}",
                    row["alpha"],
                    row["pa"],
                    row["ip"],
                    row["age"],
                    row["seasons_used"],
                ]
            )
        )

    print("\nTop 20 early-sample eligible missing_anchor by tvp_mlb_base")
    print("rank, player_name, mlb_id, tvp_mlb_base, pa, ip, age, seasons_used, is_pitcher")
    for idx, row in enumerate(
        sorted(missing_anchor, key=lambda x: x["tvp_mlb_base"], reverse=True)[:20], 1
    ):
        print(
            format_row(
                [
                    idx,
                    row["player_name"],
                    row["mlb_id"],
                    f"{row['tvp_mlb_base']:.3f}",
                    row["pa"],
                    row["ip"],
                    row["age"],
                    row["seasons_used"],
                    row["is_pitcher"],
                ]
            )
        )


if __name__ == "__main__":
    main()
