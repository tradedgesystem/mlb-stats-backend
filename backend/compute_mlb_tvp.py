from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from backend.contracts import ContractSchedule, build_contract_schedule
from backend.durability import DurabilityConfig, DurabilityInputs, build_mixture
from backend.output import PlayerOutput, build_breakdown, emit_outputs, emit_ranked_outputs
from backend.projections import AgingCurve, SeasonHistory, build_rate_projection, expected_war_path
from backend.service_time import (
    ControlTimeline,
    SERVICE_DAYS_PER_YEAR,
    SeasonWindow,
    ServiceTimeRecord,
    compute_super_two,
    control_timeline,
    remaining_games_fraction,
    super_two_for_snapshot,
)
from backend.simulate import SimulationConfig, SimulationInputs, simulate_tvp

REPO_ROOT = Path(__file__).resolve().parents[1]
VERIFIED_EXTENSIONS_PATH = REPO_ROOT / "backend" / "config" / "verified_extensions.json"


@dataclass(frozen=True)
class MLBV1Config:
    war_source: str
    price_P0: float
    price_growth: float
    discount_rate: float
    simulations: int
    year_shock_sd_hit: float
    year_shock_sd_pitch: float
    talent_sd: float
    k_rate: dict[str, float]
    k_usage: dict[str, float]
    rate_prior: dict[str, float]
    usage_prior: dict[str, float]
    aging_curve_hit: AgingCurve
    aging_curve_pitch: AgingCurve
    min_salary_m: float
    min_salary_growth: float
    arb_share: list[float]
    small_sample_pa: int
    small_sample_ip: int
    season_window: SeasonWindow
    durability_hit: DurabilityConfig
    durability_pitch: DurabilityConfig
    sp_prob_by_age: dict[int, float]
    contract_cost_basis: str
    contract_deferral_multiplier: float
    contract_overrides: dict[int, dict[str, Any]]
    hybrid_default_role: str
    metric_enabled: bool
    ops_plus_coef: float
    fip_coef: float
    metric_cap_hitter: float
    metric_cap_pitcher: float
    min_ops_pa_total: float
    min_fip_ip_total: float
    pa_trivial_max: int
    ip_role_min: int
    pa_hyb_min: int
    ip_hyb_min: int
    leaderboard_min_pa: int
    leaderboard_min_ip: int
    leaderboard_min_service_days: int
    risk_aversion_lambda: float
    service_time_zero_max_pct: float
    leaderboard_rank_by: str
    p0_window_years: int
    p0_aav_min: float
    p0_years_min: int
    p0_war_min: float
    p0_exclude_relief: bool
    p0_trim_pct: float
    verified_extension_ids: set[int]


def load_config(path: Path, war_source: str) -> MLBV1Config:
    with path.open("r") as handle:
        data = json.load(handle)
    cfg = data.get("mlb_v1") or {}
    war_sources = cfg.get("war_sources", {})
    if war_source not in war_sources:
        raise ValueError(f"Unknown war source: {war_source}")
    price_cfg = war_sources[war_source]

    def curve_from_dict(payload: dict[str, Any]) -> AgingCurve:
        return AgingCurve(
            peak_age=int(payload.get("peak_age", 27)),
            rate_delta_before=float(payload.get("rate_delta_before", 0.0)),
            rate_delta_after=float(payload.get("rate_delta_after", 0.0)),
            usage_delta_before=float(payload.get("usage_delta_before", 0.0)),
            usage_delta_after=float(payload.get("usage_delta_after", 0.0)),
        )

    def durability_from_dict(payload: dict[str, Any]) -> DurabilityConfig:
        return DurabilityConfig(
            full=float(payload.get("full", 0.8)),
            partial=float(payload.get("partial", 0.15)),
            lost=float(payload.get("lost", 0.05)),
            partial_multiplier=float(payload.get("partial_multiplier", 0.5)),
            lost_multiplier=float(payload.get("lost_multiplier", 0.0)),
            age_risk_per_year=float(payload.get("age_risk_per_year", 0.01)),
            workload_spike_penalty=float(payload.get("workload_spike_penalty", 0.03)),
        )

    season_start = cfg.get("season_start", "04-01")
    season_end = cfg.get("season_end", "10-01")

    def parse_md(md: str) -> tuple[int, int]:
        parts = md.split("-")
        return int(parts[0]), int(parts[1])

    start_m, start_d = parse_md(season_start)
    end_m, end_d = parse_md(season_end)

    p0_cfg = cfg.get("p0_calibration", {})
    verified_ids: set[int] = set()
    if VERIFIED_EXTENSIONS_PATH.exists():
        try:
            data = json.loads(VERIFIED_EXTENSIONS_PATH.read_text())
            if isinstance(data, dict):
                raw_ids = data.get("ids", [])
            else:
                raw_ids = data
            for item in raw_ids or []:
                try:
                    verified_ids.add(int(item))
                except (TypeError, ValueError):
                    continue
        except json.JSONDecodeError:
            verified_ids = set()

    year_shock_cfg = cfg.get("year_shock_sd", 0.35)
    if isinstance(year_shock_cfg, dict):
        hit_shock = float(year_shock_cfg.get("H", year_shock_cfg.get("hit", 0.35)))
        pitch_shock = float(year_shock_cfg.get("P", year_shock_cfg.get("pitch", hit_shock)))
    else:
        hit_shock = float(year_shock_cfg)
        pitch_shock = float(year_shock_cfg)

    return MLBV1Config(
        war_source=war_source,
        price_P0=float(price_cfg.get("P0", 12.0)),
        price_growth=float(price_cfg.get("g", 0.0)),
        discount_rate=float(cfg.get("discount_rate", 0.15)),
        simulations=int(cfg.get("simulations", 200)),
        year_shock_sd_hit=hit_shock,
        year_shock_sd_pitch=pitch_shock,
        talent_sd=float(cfg.get("talent_sd", 0.5)),
        k_rate={k: float(v) for k, v in cfg.get("k_rate", {}).items()},
        k_usage={k: float(v) for k, v in cfg.get("k_usage", {}).items()},
        rate_prior={k: float(v) for k, v in cfg.get("rate_prior", {}).items()},
        usage_prior={k: float(v) for k, v in cfg.get("usage_prior", {}).items()},
        aging_curve_hit=curve_from_dict(cfg.get("aging_curve", {}).get("H", {})),
        aging_curve_pitch=curve_from_dict(cfg.get("aging_curve", {}).get("P", {})),
        min_salary_m=float(cfg.get("min_salary_m", 0.8)),
        min_salary_growth=float(cfg.get("min_salary_growth", 0.03)),
        arb_share=[float(v) for v in cfg.get("arb_share", [0.4, 0.6, 0.8])],
        small_sample_pa=int(cfg.get("small_sample_pa", 200)),
        small_sample_ip=int(cfg.get("small_sample_ip", 40)),
        season_window=SeasonWindow(
            start=date(2000, start_m, start_d),
            end=date(2000, end_m, end_d),
        ),
        durability_hit=durability_from_dict(cfg.get("durability", {}).get("hitters", {})),
        durability_pitch=durability_from_dict(cfg.get("durability", {}).get("pitchers", {})),
        sp_prob_by_age={int(k): float(v) for k, v in cfg.get("role_prior", {}).get("sp_prob_by_age", {}).items()},
        contract_cost_basis=str(cfg.get("contract_cost_basis", "yearly")),
        contract_deferral_multiplier=float(cfg.get("contract_deferral_multiplier", 1.3)),
        contract_overrides={
            int(k): v
            for k, v in (cfg.get("contract_overrides") or {}).items()
            if isinstance(k, (int, str)) and str(k).isdigit()
        },
        hybrid_default_role=str(cfg.get("hybrid_default_role", "H")),
        metric_enabled=bool(cfg.get("metric_enabled", True)),
        ops_plus_coef=float(cfg.get("ops_plus_coef", 0.02)),
        fip_coef=float(cfg.get("fip_coef", 0.3)),
        metric_cap_hitter=float(cfg.get("metric_cap_hitter", 0.25)),
        metric_cap_pitcher=float(cfg.get("metric_cap_pitcher", 0.25)),
        min_ops_pa_total=float(cfg.get("min_ops_pa_total", 400.0)),
        min_fip_ip_total=float(cfg.get("min_fip_ip_total", 80.0)),
        pa_trivial_max=int(cfg.get("pa_trivial_max", 30)),
        ip_role_min=int(cfg.get("ip_role_min", 20)),
        pa_hyb_min=int(cfg.get("pa_hyb_min", 200)),
        ip_hyb_min=int(cfg.get("ip_hyb_min", 20)),
        leaderboard_min_pa=int(cfg.get("leaderboard_min_pa", 200)),
        leaderboard_min_ip=int(cfg.get("leaderboard_min_ip", 50)),
        leaderboard_min_service_days=int(cfg.get("leaderboard_min_service_days", 172)),
        risk_aversion_lambda=float(cfg.get("risk_aversion_lambda", 0.5)),
        service_time_zero_max_pct=float(cfg.get("service_time_zero_max_pct", 0.05)),
        leaderboard_rank_by=str(cfg.get("leaderboard_rank_by", "tvp_risk_adj")),
        p0_window_years=int(p0_cfg.get("window_years", 2)),
        p0_aav_min=float(p0_cfg.get("aav_min", 10.0)),
        p0_years_min=int(p0_cfg.get("years_min", 2)),
        p0_war_min=float(p0_cfg.get("war_min", 1.0)),
        p0_exclude_relief=bool(p0_cfg.get("exclude_relief", True)),
        p0_trim_pct=float(p0_cfg.get("trim_pct", 0.1)),
        verified_extension_ids=verified_ids,
    )


def load_service_time(db_path: Path) -> dict[int, ServiceTimeRecord]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT mlbam_id, service_time_years, service_time_days, service_time_label "
            "FROM service_time_bref"
        )
    except sqlite3.Error:
        conn.close()
        return {}
    records = {}
    for mlbam_id, years, days, label in cur.fetchall():
        if mlbam_id is None:
            continue
        records[int(mlbam_id)] = ServiceTimeRecord(
            mlbam_id=int(mlbam_id),
            service_time_years=int(years or 0),
            service_time_days=int(days or 0),
            service_time_label=label,
        )
    conn.close()
    return records


def load_positions_map(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    with path.open() as handle:
        data = json.load(handle)
    positions: dict[int, str] = {}
    for key, value in data.items():
        try:
            mlbam_id = int(key)
        except (TypeError, ValueError):
            continue
        if isinstance(value, dict):
            pos = value.get("position")
        else:
            pos = value
        if pos:
            positions[mlbam_id] = str(pos)
    return positions


def load_war_data(path: Path) -> dict[int, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing WAR data: {path}")
    with path.open() as handle:
        data = json.load(handle)
    players = data.get("players", [])
    return {int(p["player_id"]): p for p in players if p.get("player_id") is not None}


def load_contracts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing contract data: {path}")
    with path.open() as handle:
        data = json.load(handle)
    return data.get("players", [])


def load_usage_stats(db_path: Path, seasons: list[int]) -> dict[int, dict[int, dict[str, float]]]:
    usage: dict[int, dict[int, dict[str, float]]] = {}
    if not db_path.exists():
        return usage
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(batting_stats)")
    bat_cols = {row[1] for row in cur.fetchall()}
    cur.execute("PRAGMA table_info(pitching_stats)")
    pit_cols = {row[1] for row in cur.fetchall()}

    has_ops_plus = "ops_plus" in bat_cols
    has_ab = "ab" in bat_cols
    has_obp = "obp" in bat_cols
    has_slg = "slg" in bat_cols

    has_fip = "fip" in pit_cols
    has_hr = "hr" in pit_cols
    has_bb = "bb" in pit_cols
    has_hbp = "hbp" in pit_cols
    has_so = "so" in pit_cols
    has_er = "er" in pit_cols

    for season in seasons:
        bat_fields = ["mlbid", "pa"]
        if has_ab:
            bat_fields.append("ab")
        if has_obp:
            bat_fields.append("obp")
        if has_slg:
            bat_fields.append("slg")
        if has_ops_plus:
            bat_fields.append("ops_plus")
        cur.execute(
            f"SELECT {', '.join(bat_fields)} FROM batting_stats WHERE season = ? AND lev LIKE 'Maj-%'",
            (season,),
        )
        bat_rows = cur.fetchall()
        bat_idx = {field: idx for idx, field in enumerate(bat_fields)}

        lg_obp = None
        lg_slg = None
        if has_obp and has_slg and has_ab:
            total_pa = sum((row[bat_idx["pa"]] or 0.0) for row in bat_rows)
            total_ab = sum((row[bat_idx["ab"]] or 0.0) for row in bat_rows)
            if total_pa > 0 and total_ab > 0:
                obp_sum = 0.0
                slg_sum = 0.0
                for row in bat_rows:
                    pa = row[bat_idx["pa"]] or 0.0
                    ab = row[bat_idx["ab"]] or 0.0
                    obp = row[bat_idx["obp"]] if has_obp else None
                    slg = row[bat_idx["slg"]] if has_slg else None
                    if obp is not None:
                        obp_sum += (obp or 0.0) * pa
                    if slg is not None:
                        slg_sum += (slg or 0.0) * ab
                lg_obp = obp_sum / total_pa if total_pa > 0 else None
                lg_slg = slg_sum / total_ab if total_ab > 0 else None

        for row in bat_rows:
            mlbid = row[bat_idx["mlbid"]]
            if mlbid is None:
                continue
            mlbam_id = int(mlbid)
            entry = usage.setdefault(mlbam_id, {}).setdefault(season, {})
            pa = float(row[bat_idx["pa"]] or 0.0)
            entry["pa"] = pa
            ab = row[bat_idx["ab"]] if has_ab else None
            if has_ab:
                entry["ab"] = float(ab or 0.0)
            obp = row[bat_idx["obp"]] if has_obp else None
            if has_obp:
                entry["obp"] = float(obp) if obp is not None else None
            slg = row[bat_idx["slg"]] if has_slg else None
            if has_slg:
                entry["slg"] = float(slg) if slg is not None else None
            ops_plus = row[bat_idx["ops_plus"]] if has_ops_plus else None

            if ops_plus is None and lg_obp and lg_slg and obp is not None and slg is not None:
                ops_plus = 100 * ((obp / lg_obp) + (slg / lg_slg) - 1.0)
            if ops_plus is not None:
                entry["ops_plus"] = float(ops_plus)

        pit_fields = ["mlbid", "ip", "g", "gs"]
        if has_fip:
            pit_fields.append("fip")
        if has_hr:
            pit_fields.append("hr")
        if has_bb:
            pit_fields.append("bb")
        if has_hbp:
            pit_fields.append("hbp")
        if has_so:
            pit_fields.append("so")
        if has_er:
            pit_fields.append("er")
        cur.execute(
            f"SELECT {', '.join(pit_fields)} FROM pitching_stats WHERE season = ? AND lev LIKE 'Maj-%'",
            (season,),
        )
        pit_rows = cur.fetchall()
        pit_idx = {field: idx for idx, field in enumerate(pit_fields)}

        lg_fip = None
        fip_const = None
        if (not has_fip) and has_hr and has_bb and has_hbp and has_so and has_er:
            lg_ip = sum((row[pit_idx["ip"]] or 0.0) for row in pit_rows)
            if lg_ip > 0:
                hr_sum = sum((row[pit_idx["hr"]] or 0.0) for row in pit_rows) if has_hr else 0.0
                bb_sum = sum((row[pit_idx["bb"]] or 0.0) for row in pit_rows) if has_bb else 0.0
                hbp_sum = sum((row[pit_idx["hbp"]] or 0.0) for row in pit_rows) if has_hbp else 0.0
                so_sum = sum((row[pit_idx["so"]] or 0.0) for row in pit_rows) if has_so else 0.0
                er_sum = sum((row[pit_idx["er"]] or 0.0) for row in pit_rows) if has_er else 0.0
                lg_era = (9.0 * er_sum / lg_ip) if lg_ip > 0 else 0.0
                fip_const = lg_era - ((13 * hr_sum + 3 * (bb_sum + hbp_sum) - 2 * so_sum) / lg_ip)
                lg_fip = lg_era
        elif has_fip:
            lg_ip = sum((row[pit_idx["ip"]] or 0.0) for row in pit_rows)
            if lg_ip > 0:
                fip_index = pit_idx["fip"]
                fip_sum = sum((row[fip_index] or 0.0) * (row[pit_idx["ip"]] or 0.0) for row in pit_rows)
                lg_fip = fip_sum / lg_ip if lg_ip > 0 else None

        for row in pit_rows:
            mlbid = row[pit_idx["mlbid"]]
            if mlbid is None:
                continue
            mlbam_id = int(mlbid)
            entry = usage.setdefault(mlbam_id, {}).setdefault(season, {})
            ip = float(row[pit_idx["ip"]] or 0.0)
            entry["ip"] = ip
            entry["g"] = float(row[pit_idx["g"]] or 0.0)
            entry["gs"] = float(row[pit_idx["gs"]] or 0.0)
            fip_val = row[pit_idx["fip"]] if has_fip else None
            hr = row[pit_idx["hr"]] if has_hr else None
            bb = row[pit_idx["bb"]] if has_bb else None
            hbp = row[pit_idx["hbp"]] if has_hbp else None
            so = row[pit_idx["so"]] if has_so else None
            er = row[pit_idx["er"]] if has_er else None

            if fip_val is None and fip_const is not None and ip > 0 and hr is not None and bb is not None and hbp is not None and so is not None:
                fip_val = ((13 * hr + 3 * (bb + hbp) - 2 * so) / ip) + fip_const
            if fip_val is not None:
                entry["fip"] = float(fip_val)
            if lg_fip is not None:
                entry["lg_fip"] = float(lg_fip)

    conn.close()
    return usage




def coverage_ok(db_path: Path, expected_seasons: list[int]) -> bool:
    if not db_path.exists():
        return False
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT season FROM batting_stats")
        batting_seasons = {row[0] for row in cur.fetchall() if row[0] is not None}
        cur.execute("SELECT DISTINCT season FROM pitching_stats")
        pitching_seasons = {row[0] for row in cur.fetchall() if row[0] is not None}
    except sqlite3.Error:
        conn.close()
        return False
    conn.close()
    expected = set(expected_seasons)
    return expected.issubset(batting_seasons) and expected.issubset(pitching_seasons)


def total_usage(usage: dict[int, dict[str, float]]) -> tuple[float, float]:
    pa_total = sum(v.get("pa", 0.0) for v in usage.values())
    ip_total = sum(v.get("ip", 0.0) for v in usage.values())
    return pa_total, ip_total


def usage_window_seasons_present(usage: dict[int, dict[str, float]]) -> int:
    return sum(
        1
        for season in usage.values()
        if (season.get("pa", 0.0) > 0) or (season.get("ip", 0.0) > 0)
    )


def risk_adjusted_value(mean: float, std: float, risk_lambda: float) -> float:
    return mean - (risk_lambda * std)


def weighted_metric_avg(
    usage: dict[int, dict[str, float]],
    metric_key: str,
    weight_key: str,
) -> tuple[float | None, float]:
    total_weight = 0.0
    total = 0.0
    for season in usage.values():
        metric = season.get(metric_key)
        weight = season.get(weight_key, 0.0) or 0.0
        if metric is None or weight <= 0:
            continue
        total += float(metric) * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return None, 0.0
    return total / total_weight, total_weight


def clamp_metric_adjustment(raw: float, baseline: float, cap_fraction: float) -> float:
    cap_fraction = max(0.0, min(cap_fraction, 0.30))
    cap = abs(baseline) * cap_fraction
    if cap <= 0:
        return 0.0
    return max(-cap, min(cap, raw))


def build_status_t(
    timeline: ControlTimeline,
    schedule: ContractSchedule,
) -> list[str]:
    statuses: list[str] = []
    year_types = [year.year_type for year in timeline.years]
    for idx, year in enumerate(schedule.years):
        basis = year.basis
        if basis in {"guaranteed", "aav", "cbt_aav"}:
            statuses.append("contract")
            continue
        if basis == "option":
            statuses.append("option")
            continue
        if basis == "model_cost_prearb":
            statuses.append("prearb")
            continue
        if basis == "model_cost_arb":
            statuses.append("arb")
            continue
        if idx < len(year_types):
            statuses.append(year_types[idx])
            continue
        statuses.append("fa_contract" if basis == "fa" else basis)
    return statuses


def backloaded_contract(schedule: ContractSchedule, threshold: float = 1.25) -> bool:
    costs = [
        year.cost_m
        for year in schedule.years
        if year.basis in {"guaranteed", "aav", "cbt_aav", "option"}
    ]
    if len(costs) < 6:
        return False
    first = sum(costs[:3]) / 3.0
    last = sum(costs[-3:]) / 3.0
    if first <= 0:
        return False
    return last > (first * threshold)


def late_negative_surplus_years(breakdown: list[dict[str, Any]], tail_years: int = 3) -> int:
    if not breakdown:
        return 0
    tail = breakdown[-tail_years:]
    return sum(1 for row in tail if row.get("surplus", 0.0) < 0.0)


def seasons_with_usage(history: list[SeasonHistory]) -> int:
    return sum(1 for entry in history if entry.usage > 0)


def is_player_eligible(service_record: ServiceTimeRecord | None, usage: dict[int, dict[str, float]]) -> bool:
    service_days = service_record.total_service_days if service_record else 0
    pa_total, ip_total = total_usage(usage)
    return service_days > 0 or (pa_total + ip_total) > 0


def leaderboard_eligible(
    role: str,
    usage: dict[int, dict[str, float]],
    service_record: ServiceTimeRecord | None,
    config: MLBV1Config,
) -> bool:
    service_days = service_record.total_service_days if service_record else 0
    if service_days >= config.leaderboard_min_service_days:
        return True
    pa_total, ip_total = total_usage(usage)
    if role in {"SP", "RP"}:
        return ip_total >= config.leaderboard_min_ip
    if role == "H":
        return pa_total >= config.leaderboard_min_pa
    if role == "HYB":
        return (pa_total >= config.leaderboard_min_pa) or (ip_total >= config.leaderboard_min_ip)
    return False


def sp_prob_by_age(age: int, mapping: dict[int, float]) -> float:
    if not mapping:
        return 0.5
    candidates = sorted(mapping.items())
    chosen = candidates[0][1]
    for age_key, value in candidates:
        if age >= age_key:
            chosen = value
        else:
            break
    return max(0.0, min(1.0, chosen))


def determine_role(usage: dict[int, dict[str, float]], config: MLBV1Config) -> tuple[str, float | None]:
    pa = sum(v.get("pa", 0.0) for v in usage.values())
    ip = sum(v.get("ip", 0.0) for v in usage.values())
    if ip >= config.ip_role_min and pa <= config.pa_trivial_max:
        g = sum(v.get("g", 0.0) for v in usage.values())
        gs = sum(v.get("gs", 0.0) for v in usage.values())
        gs_share = (gs / g) if g else 0.0
        role = "SP" if gs_share >= 0.5 else "RP"
        return role, gs_share
    if ip >= config.ip_hyb_min and pa >= config.pa_hyb_min:
        return "HYB", None
    if ip > 0 and pa == 0:
        g = sum(v.get("g", 0.0) for v in usage.values())
        gs = sum(v.get("gs", 0.0) for v in usage.values())
        gs_share = (gs / g) if g else 0.0
        role = "SP" if gs_share >= 0.5 else "RP"
        return role, gs_share
    if pa > 0 and ip == 0:
        return "H", None
    if ip >= config.ip_hyb_min:
        g = sum(v.get("g", 0.0) for v in usage.values())
        gs = sum(v.get("gs", 0.0) for v in usage.values())
        gs_share = (gs / g) if g else 0.0
        role = "SP" if gs_share >= 0.5 else "RP"
        return role, gs_share
    if pa >= config.pa_hyb_min:
        return "H", None
    return "H", None


def resolve_projection_role(
    role_code: str,
    usage: dict[int, dict[str, float]],
    gs_share: float | None,
    config: MLBV1Config,
) -> str:
    if role_code != "HYB":
        return role_code
    resolved, _ = determine_role(usage, config)
    if resolved in {"SP", "RP", "H"}:
        return resolved
    return config.hybrid_default_role if config.hybrid_default_role in {"H", "SP", "RP"} else "H"


def should_use_aav_for_deferrals(
    contract: dict[str, Any],
    snapshot_year: int,
    multiplier: float,
) -> bool:
    aav = contract.get("aav_m")
    if aav is None:
        return False
    years = [year for year in (contract.get("contract_years") or []) if year.get("season") and year["season"] >= snapshot_year]
    if not years:
        return False
    total_cash = sum(float(year.get("salary_m") or 0.0) for year in years)
    avg_cash = total_cash / len(years) if years else 0.0
    if avg_cash <= 0:
        return False
    return (float(aav) / avg_cash) >= multiplier


def apply_contract_overrides(
    contract: dict[str, Any],
    mlbam_id: int,
    snapshot_year: int,
    config: MLBV1Config,
) -> tuple[dict[str, Any], str | None]:
    contract_copy = dict(contract)
    basis_override: str | None = None
    override = config.contract_overrides.get(mlbam_id)
    if override:
        basis_override = str(override.get("basis") or "aav_override")
        term_start = override.get("term_start")
        term_years = override.get("term_years")
        if term_start is not None and term_years is not None:
            try:
                term_start = int(term_start)
                term_years = int(term_years)
                remaining = max(0, term_start + term_years - snapshot_year)
                contract_copy["years_remaining"] = remaining
                contract_copy["guaranteed_years_remaining"] = remaining
            except (TypeError, ValueError):
                pass
        if override.get("years_remaining") is not None:
            try:
                remaining = int(override.get("years_remaining"))
                contract_copy["years_remaining"] = remaining
                contract_copy["guaranteed_years_remaining"] = remaining
            except (TypeError, ValueError):
                pass
        if override.get("aav_m") is not None:
            try:
                aav = float(override.get("aav_m"))
                years = int(contract_copy.get("years_remaining") or 0)
                contract_copy["contract_years"] = [
                    {"season": snapshot_year + i, "salary_m": aav, "is_guaranteed": True}
                    for i in range(years)
                ]
            except (TypeError, ValueError):
                pass
        contract_copy["cost_basis_override"] = basis_override
        return contract_copy, basis_override

    if config.contract_cost_basis == "aav_for_deferrals" and should_use_aav_for_deferrals(
        contract_copy,
        snapshot_year,
        config.contract_deferral_multiplier,
    ):
        aav = contract_copy.get("aav_m")
        years = contract_copy.get("years_remaining") or contract_copy.get("guaranteed_years_remaining")
        if aav is not None and years:
            try:
                years = int(years)
                aav = float(aav)
                contract_copy["contract_years"] = [
                    {"season": snapshot_year + i, "salary_m": aav, "is_guaranteed": True}
                    for i in range(years)
                ]
                basis_override = "aav"
                contract_copy["cost_basis_override"] = basis_override
            except (TypeError, ValueError):
                pass
    return contract_copy, basis_override


def usage_prior_for_player(
    role: str,
    usage: dict[int, dict[str, float]],
    config: MLBV1Config,
) -> float:
    pa_total, ip_total = total_usage(usage)
    if role in {"SP", "RP"}:
        if ip_total < config.small_sample_ip:
            return config.usage_prior.get("rp", 60.0)
        return config.usage_prior.get("sp", 180.0) if role == "SP" else config.usage_prior.get("rp", 60.0)
    if pa_total < config.small_sample_pa:
        return config.usage_prior.get("bench", 150.0)
    if pa_total >= 500:
        return config.usage_prior.get("everyday", 600.0)
    if pa_total >= 250:
        return config.usage_prior.get("platoon", 350.0)
    return config.usage_prior.get("bench", 150.0)


def contract_source_label(contract: dict[str, Any], verified: bool, override: bool) -> str | None:
    if override:
        return "override"
    if verified:
        return "verified_extension"
    source_url = contract.get("source_url")
    if isinstance(source_url, str):
        lower = source_url.lower()
        if "spotrac" in lower:
            return "spotrac"
        if "cot" in lower:
            return "cot"
        return "url"
    return None


def contract_confidence_label(contract_source: str | None, verified: bool, ignored: bool) -> str:
    if ignored:
        return "low"
    if verified:
        return "high"
    if contract_source in {"spotrac", "url"}:
        return "low"
    return "high"


def is_prospect_like(
    service_days: int,
    seasons_present: int,
    pa_total: float,
    ip_total: float,
    config: MLBV1Config,
) -> bool:
    if service_days >= SERVICE_DAYS_PER_YEAR:
        return False
    low_usage = (pa_total < config.leaderboard_min_pa) and (ip_total < config.leaderboard_min_ip)
    return seasons_present < 2 or low_usage


def build_price_curve(config: MLBV1Config, horizon: int) -> list[float]:
    # TODO: support P0_by_year mapping in config for WAR source calibration.
    return [config.price_P0 * ((1.0 + config.price_growth) ** t) for t in range(horizon)]


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    n = len(values_sorted)
    mid = n // 2
    if n % 2 == 1:
        return values_sorted[mid]
    return (values_sorted[mid - 1] + values_sorted[mid]) / 2.0


def _trimmed(values: list[float], trim_pct: float) -> list[float]:
    if not values:
        return []
    trim_pct = max(0.0, min(0.49, trim_pct))
    values_sorted = sorted(values)
    trim = int(len(values_sorted) * trim_pct)
    if trim == 0 or len(values_sorted) <= trim * 2:
        return values_sorted
    return values_sorted[trim:-trim]


def contract_start_year(contract: dict[str, Any]) -> int | None:
    years = [y.get("season") for y in (contract.get("contract_years") or []) if y.get("season")]
    if not years:
        return None
    return int(min(years))


def guaranteed_years(contract: dict[str, Any]) -> int:
    years = [
        y
        for y in (contract.get("contract_years") or [])
        if y.get("season") and (y.get("is_guaranteed") is True or y.get("is_guaranteed") is None)
    ]
    if years:
        return len(years)
    for key in ("guaranteed_years_remaining", "years_remaining"):
        if contract.get(key) is not None:
            try:
                return int(contract.get(key) or 0)
            except (TypeError, ValueError):
                continue
    return 0


def resolve_market_aav(
    contract: dict[str, Any],
    mlbam_id: int,
    snapshot_year: int,
    config: MLBV1Config,
) -> float | None:
    override = config.contract_overrides.get(mlbam_id)
    if override and override.get("aav_m") is not None:
        try:
            return float(override.get("aav_m"))
        except (TypeError, ValueError):
            pass
    aav = contract.get("aav_m")
    if aav is not None:
        try:
            return float(aav)
        except (TypeError, ValueError):
            pass
    total = contract.get("total_value_m")
    years = guaranteed_years(contract)
    if total is not None and years:
        try:
            return float(total) / years
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    salaries = [
        y.get("salary_m")
        for y in (contract.get("contract_years") or [])
        if y.get("salary_m") is not None and (y.get("is_guaranteed") is True or y.get("is_guaranteed") is None)
    ]
    if salaries:
        return sum(float(s) for s in salaries) / len(salaries)
    if (
        config.contract_cost_basis == "aav_for_deferrals"
        and should_use_aav_for_deferrals(contract, snapshot_year, config.contract_deferral_multiplier)
        and contract.get("aav_m") is not None
    ):
        try:
            return float(contract.get("aav_m"))
        except (TypeError, ValueError):
            return None
    return None


def projected_war_next_year(
    player: dict[str, Any],
    config: MLBV1Config,
    snapshot_year: int,
) -> tuple[str, float] | None:
    age = player.get("age")
    if age is None:
        return None
    age = int(age)
    role_code = player.get("role")
    usage = player.get("usage", {})
    war_entry = player.get("war", {})
    gs_share = player.get("gs_share")
    projection_role = resolve_projection_role(role_code, usage, gs_share, config)

    history: list[SeasonHistory] = []
    for season in [snapshot_year - 3, snapshot_year - 2, snapshot_year - 1]:
        war_val = war_entry.get(f"war_{season}")
        if war_val is None or isinstance(war_val, float) and math.isnan(war_val):
            war_val = 0.0
        usage_val = 0.0
        if projection_role in {"SP", "RP"}:
            usage_val = usage.get(season, {}).get("ip", 0.0)
        else:
            usage_val = usage.get(season, {}).get("pa", 0.0)
        history.append(SeasonHistory(season=season, war=float(war_val), usage=float(usage_val)))

    pa_total, ip_total = total_usage(usage)
    history_seasons = seasons_with_usage(history)
    has_track_record = history_seasons >= 2
    if projection_role in {"SP", "RP"}:
        denom = 180.0
        base_rate_prior = config.rate_prior.get(projection_role, config.rate_prior.get("SP", 2.5))
        if (ip_total < config.small_sample_ip) or (not has_track_record):
            rate_prior = 0.0
        else:
            rate_prior = base_rate_prior
        k_rate = config.k_rate.get(projection_role, config.k_rate.get("SP", 180))
        k_u = config.k_usage.get(projection_role, config.k_usage.get("SP", 180))
        aging = config.aging_curve_pitch
    else:
        denom = 600.0
        base_rate_prior = config.rate_prior.get(projection_role, config.rate_prior.get("H", 2.0))
        if (pa_total < config.small_sample_pa) or (not has_track_record):
            rate_prior = 0.0
        else:
            rate_prior = base_rate_prior
        k_rate = config.k_rate.get(projection_role, config.k_rate.get("H", 600))
        k_u = config.k_usage.get(projection_role, config.k_usage.get("H", 600))
        aging = config.aging_curve_hit

    usage_prior = usage_prior_for_player(projection_role, usage, config)
    projection = build_rate_projection(
        history,
        denom,
        rate_prior,
        k_rate,
        usage_prior,
        k_u,
    )
    expected = expected_war_path(
        projection.rate_post,
        projection.usage_post,
        age,
        1,
        denom,
        aging,
    )
    if not expected:
        return None
    return projection_role, expected[0]


def calibrate_price_P0(
    players: list[dict[str, Any]],
    config: MLBV1Config,
    snapshot_year: int,
) -> tuple[float, dict[str, Any]]:
    window_years = max(1, int(config.p0_window_years))
    calibration_years = set(range(snapshot_year - window_years + 1, snapshot_year + 1))
    implied: list[float] = []
    for player in players:
        contract = player.get("contract", {}) or {}
        if not contract.get("contract_years"):
            continue
        start_year = contract_start_year(contract)
        if start_year is None or start_year not in calibration_years:
            continue
        if guaranteed_years(contract) < config.p0_years_min:
            continue
        projection = projected_war_next_year(player, config, snapshot_year)
        if not projection:
            continue
        projection_role, war_next = projection
        if config.p0_exclude_relief and projection_role == "RP":
            continue
        if war_next < config.p0_war_min:
            continue
        aav = resolve_market_aav(contract, int(player["mlbam_id"]), snapshot_year, config)
        if aav is None or aav < config.p0_aav_min:
            continue
        implied.append(float(aav) / float(war_next))

    trimmed = _trimmed(implied, config.p0_trim_pct)
    median = _median(trimmed) if trimmed else config.price_P0
    summary = {
        "calibrated": bool(trimmed),
        "samples": len(implied),
        "samples_used": len(trimmed),
        "median": median,
        "trim_pct": config.p0_trim_pct,
        "window_years": window_years,
        "window_start_year": min(calibration_years) if calibration_years else snapshot_year,
        "window_end_year": max(calibration_years) if calibration_years else snapshot_year,
        "filters": {
            "aav_min": config.p0_aav_min,
            "years_min": config.p0_years_min,
            "war_min": config.p0_war_min,
            "exclude_relief": config.p0_exclude_relief,
        },
    }
    return median, summary

def compute_talent_value_p50(
    war_path: list[float],
    price_by_year: list[float],
    discount_rate: float,
    in_season_fraction: float,
) -> float:
    total = 0.0
    for t, war_t in enumerate(war_path):
        if t == 0:
            war_t *= in_season_fraction
        value = war_t * price_by_year[t]
        discount = 1.0 / ((1.0 + discount_rate) ** t)
        total += value * discount
    return total


def build_snapshot_players(
    snapshot_year: int,
    war_source: str,
    data_dir: Path,
    db_path: Path,
    config: MLBV1Config,
) -> list[dict[str, Any]]:
    war_path = data_dir / f"war_3years_{snapshot_year - 1}.json"
    contract_path = data_dir / f"players_with_contracts_{snapshot_year - 1}.json"

    war_data = load_war_data(war_path)
    contracts = load_contracts(contract_path)
    service_time = load_service_time(db_path)
    usage_stats = load_usage_stats(db_path, [snapshot_year - 3, snapshot_year - 2, snapshot_year - 1])
    positions = load_positions_map(REPO_ROOT / "backend" / "player_positions_fixture.json")

    players: list[dict[str, Any]] = []
    for entry in contracts:
        mlbam_id = entry.get("mlb_id")
        if mlbam_id is None:
            continue
        mlbam_id = int(mlbam_id)
        war_entry = war_data.get(mlbam_id)
        if not war_entry:
            continue
        usage = usage_stats.get(mlbam_id, {})
        role, gs_share = determine_role(usage, config)
        service_record = service_time.get(mlbam_id)
        if not is_player_eligible(service_record, usage):
            continue
        players.append(
            {
                "mlbam_id": mlbam_id,
                "name": entry.get("player_name"),
                "team": entry.get("team"),
                "age": entry.get("age"),
                "contract": entry.get("contract", {}),
                "war": war_entry,
                "usage": usage,
                "role": role,
                "gs_share": gs_share,
                "service_time": service_record,
                "position": positions.get(mlbam_id),
            }
        )
    return players


def print_sanity(player: PlayerOutput, raw_player: dict[str, Any] | None = None) -> None:
    print(f"\n[sanity] {player.name} ({player.mlbam_id})")
    print(
        f"  tvp_p50={player.tvp_p50:.2f} tvp_mean={player.tvp_mean:.2f} "
        f"tvp_std={player.tvp_std:.2f} tvp_risk_adj={player.tvp_risk_adj:.2f}"
    )
    if player.talent_value_p50 is not None:
        print(f"  talent_value_p50={player.talent_value_p50:.2f}")
    if player.contract_source or player.contract_confidence:
        print(f"  contract_source={player.contract_source} contract_confidence={player.contract_confidence}")
    contract = (raw_player or {}).get("contract", {}) if raw_player else {}
    contract_id = contract.get("contract_id") or "none"
    print(f"  contract_id={contract_id}")
    if contract:
        print(
            "  contract_summary="
            f"aav_m={contract.get('aav_m')} total_value_m={contract.get('total_value_m')} "
            f"years_remaining={contract.get('years_remaining')} "
            f"guaranteed_years_remaining={contract.get('guaranteed_years_remaining')} "
            f"free_agent_year={contract.get('free_agent_year')}"
        )
        if contract.get("source_url"):
            print(f"  contract_source_url={contract.get('source_url')}")
    horizon = len(player.breakdown)
    pv_value_total = 0.0
    pv_cost_total = 0.0
    for row in player.breakdown:
        value = row["war"] * row["price"]
        pv_value_total += value * row["discount"]
        pv_cost_total += row["cost"] * row["discount"]
    print(f"  horizon_years={horizon} pv_value_total={pv_value_total:.2f} pv_cost_total={pv_cost_total:.2f}")
    print("  year  war   price  value  cost  surplus  pv  basis")
    for row in player.breakdown:
        value = row["war"] * row["price"]
        print(
            f"  {row['season']}  "
            f"{row['war']:.3f}  {row['price']:.2f}  {value:.2f}  "
            f"{row['cost']:.2f}  {row['surplus']:.2f}  {row['pv_surplus']:.2f}  {row['cost_basis']}"
        )


def build_player_output(
    player: dict[str, Any],
    config: MLBV1Config,
    snapshot_year: int,
    in_season_fraction: float,
    super_two_ids: set[int],
) -> PlayerOutput | None:
    mlbam_id = player["mlbam_id"]
    age = player.get("age")
    if age is None:
        return None
    age = int(age)
    role_code = player.get("role")
    usage = player.get("usage", {})
    war_entry = player.get("war", {})
    gs_share = player.get("gs_share")
    projection_role = resolve_projection_role(role_code, usage, gs_share, config)

    history: list[SeasonHistory] = []
    for season in [snapshot_year - 3, snapshot_year - 2, snapshot_year - 1]:
        war_val = war_entry.get(f"war_{season}")
        if war_val is None or isinstance(war_val, float) and math.isnan(war_val):
            war_val = 0.0
        usage_val = 0.0
        if projection_role in {"SP", "RP"}:
            usage_val = usage.get(season, {}).get("ip", 0.0)
        else:
            usage_val = usage.get(season, {}).get("pa", 0.0)
        history.append(SeasonHistory(season=season, war=float(war_val), usage=float(usage_val)))

    pa_total, ip_total = total_usage(usage)
    seasons_present = usage_window_seasons_present(usage)
    history_seasons = seasons_with_usage(history)
    has_track_record = history_seasons >= 2
    if projection_role in {"SP", "RP"}:
        denom = 180.0
        base_rate_prior = config.rate_prior.get(projection_role, config.rate_prior.get("SP", 2.5))
        if (ip_total < config.small_sample_ip) or (not has_track_record):
            rate_prior = 0.0
        else:
            rate_prior = base_rate_prior
        k_rate = config.k_rate.get(projection_role, config.k_rate.get("SP", 180))
        k_u = config.k_usage.get(projection_role, config.k_usage.get("SP", 180))
        aging = config.aging_curve_pitch
    else:
        denom = 600.0
        base_rate_prior = config.rate_prior.get(projection_role, config.rate_prior.get("H", 2.0))
        if (pa_total < config.small_sample_pa) or (not has_track_record):
            rate_prior = 0.0
        else:
            rate_prior = base_rate_prior
        k_rate = config.k_rate.get(projection_role, config.k_rate.get("H", 600))
        k_u = config.k_usage.get(projection_role, config.k_usage.get("H", 600))
        aging = config.aging_curve_hit

    usage_prior = usage_prior_for_player(projection_role, usage, config)
    projection = build_rate_projection(
        history,
        denom,
        rate_prior,
        k_rate,
        usage_prior,
        k_u,
    )
    window_seasons = [snapshot_year - 3, snapshot_year - 2, snapshot_year - 1]
    window_usage = {season: usage.get(season, {}) for season in window_seasons}
    war_rate_war = projection.rate_post
    war_rate_post_final = war_rate_war
    metric_adjustment_raw = 0.0
    metric_adjustment_clamped = 0.0
    ops_plus_3yr = None
    fip_3yr = None
    lg_fip_3yr = None
    fip_delta = None

    if config.metric_enabled:
        if projection_role in {"SP", "RP"}:
            fip_3yr, ip_total_metric = weighted_metric_avg(window_usage, "fip", "ip")
            lg_fip_3yr, _ = weighted_metric_avg(window_usage, "lg_fip", "ip")
            if ip_total_metric < config.min_fip_ip_total:
                fip_3yr = None
                lg_fip_3yr = None
            if fip_3yr is not None and lg_fip_3yr is not None:
                fip_delta = lg_fip_3yr - fip_3yr
                metric_adjustment_raw = config.fip_coef * fip_delta
                metric_adjustment_clamped = clamp_metric_adjustment(
                    metric_adjustment_raw,
                    war_rate_war,
                    config.metric_cap_pitcher,
                )
        else:
            ops_plus_3yr, pa_total_metric = weighted_metric_avg(window_usage, "ops_plus", "pa")
            if pa_total_metric < config.min_ops_pa_total:
                ops_plus_3yr = None
            if ops_plus_3yr is not None:
                ops_z = ops_plus_3yr - 100.0
                metric_adjustment_raw = config.ops_plus_coef * ops_z
                metric_adjustment_clamped = clamp_metric_adjustment(
                    metric_adjustment_raw,
                    war_rate_war,
                    config.metric_cap_hitter,
                )

        war_rate_post_final = war_rate_war + metric_adjustment_clamped

    projection = replace(projection, rate_post=war_rate_post_final)

    service_record: ServiceTimeRecord | None = player.get("service_time")
    service_days_total = service_record.total_service_days if service_record else 0
    super_two = mlbam_id in super_two_ids
    timeline = control_timeline(service_days_total, super_two)

    contract_raw = player.get("contract", {}) or {}
    override_present = mlbam_id in config.contract_overrides
    verified_extension = mlbam_id in config.verified_extension_ids
    prospect_like = is_prospect_like(service_days_total, seasons_present, pa_total, ip_total, config)
    contract_ignored = prospect_like and not (override_present or verified_extension)
    contract_source = contract_source_label(contract_raw, verified_extension, override_present)
    contract_confidence = contract_confidence_label(contract_source, verified_extension or override_present, contract_ignored)
    contract_for_schedule = {} if contract_ignored else contract_raw
    contract, basis_override = apply_contract_overrides(contract_for_schedule, mlbam_id, snapshot_year, config)
    guaranteed_years_remaining = int(contract.get("guaranteed_years_remaining") or 0)
    horizon = max(guaranteed_years_remaining, timeline.team_control_years_remaining)
    if horizon == 0:
        horizon = 1

    expected_war = expected_war_path(
        projection.rate_post,
        projection.usage_post,
        age,
        horizon,
        denom,
        aging,
    )

    price_by_year = build_price_curve(config, horizon)

    schedule = build_contract_schedule(
        contract,
        snapshot_year,
        horizon,
        [year.year_type for year in timeline.years],
        expected_war,
        price_by_year,
        config.arb_share,
        config.min_salary_m,
        config.min_salary_growth,
        guaranteed_basis=basis_override,
    )

    durability = build_mixture(
        DurabilityInputs(is_pitcher=projection_role in {"SP", "RP"}, age=age),
        config.durability_hit,
        config.durability_pitch,
    )

    role_prob_sp = None
    if projection_role in {"SP", "RP"}:
        prior_sp = sp_prob_by_age(age, config.sp_prob_by_age)
        if gs_share is None:
            role_prob_sp = prior_sp
        else:
            role_prob_sp = (prior_sp + float(gs_share)) / 2.0

    shock_sd = config.year_shock_sd_pitch if projection_role in {"SP", "RP"} else config.year_shock_sd_hit
    talent_sd = config.talent_sd
    sim_config = SimulationConfig(
        sims=config.simulations,
        year_shock_sd=shock_sd,
        talent_sd=talent_sd,
    )
    sim_inputs = SimulationInputs(
        rate_post=projection.rate_post,
        usage_post=projection.usage_post,
        age=age,
        denom=denom,
        aging=aging,
        horizon_years=horizon,
        war_price_by_year=price_by_year,
        discount_rate=config.discount_rate,
        contract_years=schedule.years,
        durability=durability,
        in_season_fraction=in_season_fraction,
        role_prob_sp=role_prob_sp,
    )
    sim_result = simulate_tvp(sim_config, sim_inputs, expected_war)
    risk_adj = risk_adjusted_value(sim_result.mean, sim_result.std, config.risk_aversion_lambda)

    flags = {
        "high_defense_uncertainty": player.get("position") is None,
        "pitcher_tail_risk": projection_role in {"SP", "RP"} and any(
            state.label == "lost" and state.probability >= 0.12 for state in durability.states
        ),
        "small_sample": projection.n_usage
        < (config.small_sample_ip if projection_role in {"SP", "RP"} else config.small_sample_pa),
        "role_change_risk": role_code == "HYB"
        or (role_prob_sp is not None and 0.3 < role_prob_sp < 0.7),
    }
    components = None
    if config.metric_enabled:
        components = {
            "ops_plus_3yr": ops_plus_3yr,
            "fip_3yr": fip_3yr,
            "lg_fip_3yr": lg_fip_3yr,
            "fip_delta": fip_delta,
            "war_rate_war": war_rate_war,
            "metric_adjustment_raw": metric_adjustment_raw,
            "metric_adjustment_clamped": metric_adjustment_clamped,
            "war_rate_post_final": war_rate_post_final,
        }
    leaderboard_ok = leaderboard_eligible(role_code, usage, service_record, config)
    flags["leaderboard_eligible"] = leaderboard_ok
    flags["small_sample"] = flags["small_sample"] or (not leaderboard_ok)

    breakdown = build_breakdown(
        snapshot_year,
        sim_result.war_p50,
        schedule.years,
        price_by_year,
        config.discount_rate,
        in_season_fraction,
    )
    talent_value_p50 = compute_talent_value_p50(
        sim_result.war_p50,
        price_by_year,
        config.discount_rate,
        in_season_fraction,
    )

    status_t = build_status_t(timeline, schedule)
    late_negative_years = late_negative_surplus_years(breakdown)
    is_backloaded = backloaded_contract(schedule, threshold=1.25)

    service_time_label = service_record.service_time_label if service_record else None
    return PlayerOutput(
        mlbam_id=mlbam_id,
        name=player.get("name") or str(mlbam_id),
        team=player.get("team"),
        age=age,
        role=role_code,
        position=player.get("position"),
        status_t=status_t,
        tvp=risk_adj,
        tvp_p10=sim_result.quantiles.get("p10", 0.0),
        tvp_p50=sim_result.quantiles.get("p50", 0.0),
        tvp_p90=sim_result.quantiles.get("p90", 0.0),
        talent_value_p50=talent_value_p50,
        tvp_mean=sim_result.mean,
        tvp_std=sim_result.std,
        tvp_risk_adj=risk_adj,
        ops_plus_3yr=ops_plus_3yr,
        fip_3yr=fip_3yr,
        lg_fip_3yr=lg_fip_3yr,
        fip_delta=fip_delta,
        war_rate_war=war_rate_war,
        metric_adjustment_raw=metric_adjustment_raw,
        metric_adjustment_clamped=metric_adjustment_clamped,
        war_rate_post_final=war_rate_post_final,
        flags={
            **flags,
            "backloaded_contract": is_backloaded,
            "late_negative_surplus": late_negative_years >= 2,
            "contract_ignored_prospect_like": contract_ignored,
        },
        contract_source=contract_source,
        contract_confidence=contract_confidence,
        late_negative_surplus_years=late_negative_years,
        breakdown=breakdown,
        service_time=service_time_label,
        pa_window_total=pa_total,
        ip_window_total=ip_total,
        usage_window_seasons_present=seasons_present,
        components=components,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MLB TVP (v1 Surplus-Value model).")
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD snapshot date")
    parser.add_argument("--war-source", required=True, help="WAR source (e.g., bWAR)")
    parser.add_argument("--use-saved-snapshot", type=Path, default=None)
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--include-small-sample", action="store_true")
    parser.add_argument("--require-service-time", action="store_true")
    parser.add_argument(
        "--emit-both-rankers",
        action="store_true",
        help="Emit both trade-value and talent-value leaderboards.",
    )
    parser.add_argument(
        "--emit-top",
        nargs="?",
        const=50,
        type=int,
        default=None,
        help="Emit an additional top-N output (default 50).",
    )
    parser.add_argument(
        "--emit-bottom",
        nargs="?",
        const=50,
        type=int,
        default=None,
        help="Emit a bottom-N output using the chosen rank metric (default 50).",
    )
    parser.add_argument(
        "--sanity-check",
        type=str,
        default=None,
        help="Comma-separated mlbam ids or name substrings to print a breakdown.",
    )
    parser.add_argument(
        "--rank-by",
        choices=["tvp_p50", "tvp_mean", "tvp_risk_adj", "talent_value_p50"],
        default=None,
        help="Leaderboard ranking metric (default from config).",
    )
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "backend" / "tvp_config.json")
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "backend" / "stats.db")
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT / "backend" / "output")
    args = parser.parse_args()

    snapshot_date = datetime.strptime(args.snapshot_date, "%Y-%m-%d").date()
    snapshot_year = snapshot_date.year
    config = load_config(args.config, args.war_source)

    season_window = SeasonWindow(
        start=date(snapshot_year, config.season_window.start.month, config.season_window.start.day),
        end=date(snapshot_year, config.season_window.end.month, config.season_window.end.day),
    )
    in_season_fraction = remaining_games_fraction(snapshot_date, season_window)

    if args.use_saved_snapshot:
        with args.use_saved_snapshot.open() as handle:
            snapshot_data = json.load(handle)
        players = snapshot_data.get("players", [])
    else:
        players = build_snapshot_players(snapshot_year, args.war_source, args.data_dir, args.db, config)

    service_records = [p.get("service_time") for p in players if p.get("service_time")]
    service_records = [rec for rec in service_records if isinstance(rec, ServiceTimeRecord)]
    super_two = super_two_for_snapshot(service_records, snapshot_date, season_window)

    calibrated_p0, calibration_summary = calibrate_price_P0(players, config, snapshot_year)
    config = replace(config, price_P0=calibrated_p0)

    outputs: list[PlayerOutput] = []
    for player in players:
        output = build_player_output(
            player,
            config,
            snapshot_year,
            in_season_fraction,
            super_two.super_two_ids,
        )
        if output:
            outputs.append(output)

    def sort_by_metric(metric: str) -> list[PlayerOutput]:
        if metric == "tvp_mean":
            key = lambda o: o.tvp_mean
        elif metric == "tvp_risk_adj":
            key = lambda o: o.tvp_risk_adj
        elif metric == "talent_value_p50":
            key = lambda o: o.talent_value_p50
        else:
            key = lambda o: o.tvp_p50
        return sorted(outputs, key=key, reverse=True)

    rank_by = args.rank_by or config.leaderboard_rank_by
    outputs_sorted = sort_by_metric(rank_by)
    leaderboard_pool = [o for o in outputs_sorted if o.flags.get("leaderboard_eligible", True)]
    if args.include_small_sample:
        eligible_outputs = leaderboard_pool
    else:
        eligible_outputs = [o for o in leaderboard_pool if not o.flags.get("small_sample", False)]
    top = eligible_outputs[: args.top]

    zero_service_all = sum(
        1 for o in outputs_sorted if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    zero_service_lb = sum(
        1
        for o in leaderboard_pool
        if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    zero_service_top = sum(
        1
        for o in top
        if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    zero_pct_all = zero_service_all / len(outputs_sorted) if outputs_sorted else 0.0
    zero_pct_lb = zero_service_lb / len(leaderboard_pool) if leaderboard_pool else 0.0
    zero_pct_top = zero_service_top / len(top) if top else 0.0
    service_time_ok = zero_pct_lb <= config.service_time_zero_max_pct
    data_ok = coverage_ok(args.db, [snapshot_year - 3, snapshot_year - 2, snapshot_year - 1])
    pricing_suffix = f"nominal_g{config.price_growth:.2f}_d{config.discount_rate:.2f}".replace(".", "p")
    meta_extra = {
        "data_coverage_ok": data_ok,
        "service_time_zero_pct_all": round(zero_pct_all, 4),
        "service_time_zero_pct_leaderboard": round(zero_pct_lb, 4),
        "service_time_zero_pct_top50": round(zero_pct_top, 4),
        "warning_service_time_incomplete": (zero_pct_lb > config.service_time_zero_max_pct),
        "top50_unreliable": (not data_ok) or (zero_pct_top > 0.0),
        "risk_aversion_lambda": config.risk_aversion_lambda,
        "pricing_regime": "nominal",
        "P0": config.price_P0,
        "g": config.price_growth,
        "discount_rate": config.discount_rate,
        "trade_currency": "tvp_mean_minus_lambda_std",
        "p0_calibration": calibration_summary,
        "metric_enabled": config.metric_enabled,
        "ops_plus_coef": config.ops_plus_coef,
        "fip_coef": config.fip_coef,
        "metric_cap_hitter": config.metric_cap_hitter,
        "metric_cap_pitcher": config.metric_cap_pitcher,
        "min_ops_pa_total": config.min_ops_pa_total,
        "min_fip_ip_total": config.min_fip_ip_total,
    }
    if args.require_service_time and not service_time_ok:
        raise SystemExit(
            f"Service time coverage failed: {zero_pct_lb:.1%} of leaderboard players have zero service time."
        )

    json_path, csv_path = emit_outputs(
        REPO_ROOT / "backend" / "output",
        args.snapshot_date,
        args.war_source,
        top,
        args.top,
        prefix="top",
        rank_by=rank_by,
        label=f"{rank_by}_{pricing_suffix}",
        meta_extra=meta_extra,
    )

    print(f"Wrote {len(top)} players to {json_path} and {csv_path}")

    if args.emit_both_rankers:
        trade_rank = "tvp_risk_adj"
        trade_sorted = sort_by_metric(trade_rank)
        trade_pool = [o for o in trade_sorted if o.flags.get("leaderboard_eligible", True)]
        if args.include_small_sample:
            trade_eligible = trade_pool
        else:
            trade_eligible = [o for o in trade_pool if not o.flags.get("small_sample", False)]
        trade_top = trade_eligible[: args.top]
        trade_json, trade_csv = emit_outputs(
            REPO_ROOT / "backend" / "output",
            args.snapshot_date,
            args.war_source,
            trade_top,
            args.top,
            prefix="top",
            rank_by=trade_rank,
            label=f"trade_value_{pricing_suffix}",
            meta_extra=meta_extra,
        )
        print(f"Wrote {len(trade_top)} players to {trade_json} and {trade_csv}")

        talent_rank = "talent_value_p50"
        talent_sorted = sort_by_metric(talent_rank)
        talent_pool = [o for o in talent_sorted if o.flags.get("leaderboard_eligible", True)]
        if args.include_small_sample:
            talent_eligible = talent_pool
        else:
            talent_eligible = [o for o in talent_pool if not o.flags.get("small_sample", False)]
        talent_top = talent_eligible[: args.top]
        talent_json, talent_csv = emit_outputs(
            REPO_ROOT / "backend" / "output",
            args.snapshot_date,
            args.war_source,
            talent_top,
            args.top,
            prefix="top",
            rank_by=talent_rank,
            label=f"best_players_{pricing_suffix}",
            meta_extra=meta_extra,
        )
        print(f"Wrote {len(talent_top)} players to {talent_json} and {talent_csv}")

        trade_ranks = {o.mlbam_id: idx + 1 for idx, o in enumerate(trade_sorted)}
        talent_ranks = {o.mlbam_id: idx + 1 for idx, o in enumerate(talent_sorted)}
        combined_json, combined_csv = emit_ranked_outputs(
            REPO_ROOT / "backend" / "output",
            args.snapshot_date,
            args.war_source,
            trade_top,
            {"tvp_risk_adj": trade_ranks, "talent_value_p50": talent_ranks},
            args.top,
            prefix="top",
            label=f"combined_{pricing_suffix}",
            meta_extra=meta_extra,
        )
        print(f"Wrote {len(trade_top)} players to {combined_json} and {combined_csv}")

    if args.emit_top is not None:
        emit_top_n = max(1, int(args.emit_top))
        emit_top = eligible_outputs[:emit_top_n]
        extra_json, extra_csv = emit_outputs(
            REPO_ROOT / "backend" / "output",
            args.snapshot_date,
            args.war_source,
            emit_top,
            emit_top_n,
            prefix="top",
            rank_by=rank_by,
            label=f"{rank_by}_{pricing_suffix}",
            meta_extra=meta_extra,
        )
        print(f"Wrote {len(emit_top)} players to {extra_json} and {extra_csv}")

    if args.emit_bottom is not None:
        emit_bottom_n = max(1, int(args.emit_bottom))
        emit_bottom = list(reversed(eligible_outputs[-emit_bottom_n:])) if eligible_outputs else []
        bottom_json, bottom_csv = emit_outputs(
            REPO_ROOT / "backend" / "output",
            args.snapshot_date,
            args.war_source,
            emit_bottom,
            emit_bottom_n,
            prefix="bottom",
            rank_by=rank_by,
            label=f"{rank_by}_{pricing_suffix}",
            meta_extra=meta_extra,
        )
        print(f"Wrote {len(emit_bottom)} players to {bottom_json} and {bottom_csv}")

    if args.sanity_check:
        raw_lookup = {p["mlbam_id"]: p for p in players}
        tokens = [t.strip() for t in args.sanity_check.split(",") if t.strip()]
        for token in tokens:
            player = None
            if token.isdigit():
                mlbam_id = int(token)
                for out in outputs:
                    if out.mlbam_id == mlbam_id:
                        player = out
                        break
            else:
                lowered = token.lower()
                for out in outputs:
                    if lowered in out.name.lower():
                        player = out
                        break
            if not player:
                print(f"[sanity] No player match for '{token}'")
                continue
            print_sanity(player, raw_lookup.get(player.mlbam_id))


if __name__ == "__main__":
    main()
