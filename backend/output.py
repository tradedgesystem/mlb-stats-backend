from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.contracts import ContractYear


@dataclass(frozen=True)
class PlayerOutput:
    mlbam_id: int
    name: str
    team: str | None
    age: int | None
    role: str
    position: str | None
    tvp_p10: float
    tvp_p50: float
    tvp_p90: float
    tvp_mean: float | None
    tvp_std: float | None
    tvp_risk_adj: float | None
    flags: dict[str, bool]
    breakdown: list[dict[str, Any]]
    service_time: str | None
    pa_window_total: float | None = None
    ip_window_total: float | None = None
    usage_window_seasons_present: int | None = None
    components: dict[str, Any] | None = None


def build_breakdown(
    snapshot_year: int,
    war_path: list[float],
    contract_years: list[ContractYear],
    war_price_by_year: list[float],
    discount_rate: float,
    in_season_fraction: float = 1.0,
) -> list[dict[str, Any]]:
    breakdown = []
    for t, war_t in enumerate(war_path):
        year = snapshot_year + t
        price = war_price_by_year[t]
        if t == 0:
            war_t = war_t * in_season_fraction
        value = war_t * price
        cost = contract_years[t].cost_m
        if t == 0:
            cost *= in_season_fraction
        surplus = value - cost
        discount = 1.0 / ((1.0 + discount_rate) ** t)
        pv = surplus * discount
        breakdown.append(
            {
                "season": year,
                "war": war_t,
                "price": price,
                "cost": cost,
                "surplus": surplus,
                "discount": discount,
                "pv_surplus": pv,
                "cost_basis": contract_years[t].basis,
            }
        )
    return breakdown


def emit_outputs(
    output_dir: Path,
    snapshot_date: str,
    war_source: str,
    results: list[PlayerOutput],
    top_n: int,
    meta_extra: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"tvp_mlb_v1_top{top_n}_{timestamp}.json"
    csv_path = output_dir / f"tvp_mlb_v1_top{top_n}_{timestamp}.csv"

    meta = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "snapshot_date": snapshot_date,
        "war_source": war_source,
        "player_count": len(results),
        "top_n": top_n,
    }
    if meta_extra:
        meta.update(meta_extra)
    payload = {
        "meta": meta,
        "players": [
            {
                "mlbam_id": p.mlbam_id,
                "name": p.name,
                "team": p.team,
                "age": p.age,
                "role": p.role,
                "position": p.position,
                "tvp_p10": p.tvp_p10,
                "tvp_p50": p.tvp_p50,
                "tvp_p90": p.tvp_p90,
                "tvp_mean": p.tvp_mean,
                "tvp_std": p.tvp_std,
                "tvp_risk_adj": p.tvp_risk_adj,
                "flags": p.flags,
                "service_time": p.service_time,
                "breakdown": p.breakdown,
                "pa_window_total": p.pa_window_total,
                "ip_window_total": p.ip_window_total,
                "usage_window_seasons_present": p.usage_window_seasons_present,
                **({"components": p.components} if p.components is not None else {}),
            }
            for p in results
        ],
    }

    with json_path.open("w") as handle:
        json.dump(payload, handle, indent=2)

    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "mlbam_id",
                "name",
                "team",
                "age",
                "role",
                "position",
                "tvp_p10",
                "tvp_p50",
                "tvp_p90",
                "tvp_mean",
                "tvp_std",
                "tvp_risk_adj",
                "service_time",
                "pa_window_total",
                "ip_window_total",
                "usage_window_seasons_present",
                "flags",
            ],
        )
        writer.writeheader()
        for p in results:
            writer.writerow(
                {
                    "mlbam_id": p.mlbam_id,
                    "name": p.name,
                    "team": p.team,
                    "age": p.age,
                    "role": p.role,
                    "position": p.position,
                    "tvp_p10": p.tvp_p10,
                    "tvp_p50": p.tvp_p50,
                    "tvp_p90": p.tvp_p90,
                    "tvp_mean": p.tvp_mean,
                    "tvp_std": p.tvp_std,
                    "tvp_risk_adj": p.tvp_risk_adj,
                    "service_time": p.service_time,
                    "pa_window_total": p.pa_window_total,
                    "ip_window_total": p.ip_window_total,
                    "usage_window_seasons_present": p.usage_window_seasons_present,
                    "flags": json.dumps(p.flags),
                }
            )

    return json_path, csv_path
