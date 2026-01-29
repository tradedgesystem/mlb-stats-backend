from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from backend.contracts import ContractSchedule, build_contract_schedule
from backend.durability import DurabilityConfig, DurabilityInputs, build_mixture
from backend.output import PlayerOutput, build_breakdown, emit_outputs
from backend.projections import AgingCurve, SeasonHistory, build_rate_projection, expected_war_path
from backend.service_time import (
    ControlTimeline,
    SeasonWindow,
    ServiceTimeRecord,
    compute_super_two,
    control_timeline,
    remaining_games_fraction,
    super_two_for_snapshot,
)
from backend.simulate import SimulationConfig, SimulationInputs, simulate_tvp

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class MLBV1Config:
    war_source: str
    price_P0: float
    price_growth: float
    discount_rate: float
    simulations: int
    year_shock_sd: float
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
    pa_trivial_max: int
    ip_role_min: int
    pa_hyb_min: int
    ip_hyb_min: int


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

    return MLBV1Config(
        war_source=war_source,
        price_P0=float(price_cfg.get("P0", 12.0)),
        price_growth=float(price_cfg.get("g", 0.0)),
        discount_rate=float(cfg.get("discount_rate", 0.15)),
        simulations=int(cfg.get("simulations", 200)),
        year_shock_sd=float(cfg.get("year_shock_sd", 0.3)),
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
        pa_trivial_max=int(cfg.get("pa_trivial_max", 30)),
        ip_role_min=int(cfg.get("ip_role_min", 20)),
        pa_hyb_min=int(cfg.get("pa_hyb_min", 200)),
        ip_hyb_min=int(cfg.get("ip_hyb_min", 20)),
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

    for season in seasons:
        cur.execute(
            "SELECT mlbid, pa FROM batting_stats WHERE season = ?",
            (season,),
        )
        for mlbid, pa in cur.fetchall():
            if mlbid is None:
                continue
            mlbam_id = int(mlbid)
            usage.setdefault(mlbam_id, {}).setdefault(season, {})["pa"] = float(pa or 0.0)

        cur.execute(
            "SELECT mlbid, ip, g, gs FROM pitching_stats WHERE season = ?",
            (season,),
        )
        for mlbid, ip, g, gs in cur.fetchall():
            if mlbid is None:
                continue
            mlbam_id = int(mlbid)
            entry = usage.setdefault(mlbam_id, {}).setdefault(season, {})
            entry["ip"] = float(ip or 0.0)
            entry["g"] = float(g or 0.0)
            entry["gs"] = float(gs or 0.0)

    conn.close()
    return usage


def total_usage(usage: dict[int, dict[str, float]]) -> tuple[float, float]:
    pa_total = sum(v.get("pa", 0.0) for v in usage.values())
    ip_total = sum(v.get("ip", 0.0) for v in usage.values())
    return pa_total, ip_total


def seasons_with_usage(history: list[SeasonHistory]) -> int:
    return sum(1 for entry in history if entry.usage > 0)


def is_player_eligible(service_record: ServiceTimeRecord | None, usage: dict[int, dict[str, float]]) -> bool:
    service_days = service_record.total_service_days if service_record else 0
    pa_total, ip_total = total_usage(usage)
    return service_days > 0 or (pa_total + ip_total) > 0


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


def build_price_curve(config: MLBV1Config, horizon: int) -> list[float]:
    return [config.price_P0 * ((1.0 + config.price_growth) ** t) for t in range(horizon)]


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

    service_record: ServiceTimeRecord | None = player.get("service_time")
    service_days_total = service_record.total_service_days if service_record else 0
    super_two = mlbam_id in super_two_ids
    timeline = control_timeline(service_days_total, super_two)

    contract = player.get("contract", {})
    contract, basis_override = apply_contract_overrides(contract, mlbam_id, snapshot_year, config)
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

    sim_config = SimulationConfig(
        sims=config.simulations,
        year_shock_sd=config.year_shock_sd,
        talent_sd=config.talent_sd,
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

    breakdown = build_breakdown(
        snapshot_year,
        sim_result.war_p50,
        schedule.years,
        price_by_year,
        config.discount_rate,
        in_season_fraction,
    )

    service_time_label = service_record.service_time_label if service_record else None
    return PlayerOutput(
        mlbam_id=mlbam_id,
        name=player.get("name") or str(mlbam_id),
        team=player.get("team"),
        age=age,
        role=role_code,
        position=player.get("position"),
        tvp_p10=sim_result.quantiles.get("p10", 0.0),
        tvp_p50=sim_result.quantiles.get("p50", 0.0),
        tvp_p90=sim_result.quantiles.get("p90", 0.0),
        flags=flags,
        breakdown=breakdown,
        service_time=service_time_label,
        components=player.get("components"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MLB TVP (v1 Surplus-Value model).")
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD snapshot date")
    parser.add_argument("--war-source", required=True, help="WAR source (e.g., bWAR)")
    parser.add_argument("--use-saved-snapshot", type=Path, default=None)
    parser.add_argument("--top", type=int, default=50)
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

    outputs: list[PlayerOutput] = []
    for player in players:
        output = build_player_output(player, config, snapshot_year, in_season_fraction, super_two.super_two_ids)
        if output:
            outputs.append(output)

    outputs_sorted = sorted(outputs, key=lambda x: x.tvp_p50, reverse=True)
    top = outputs_sorted[: args.top]

    json_path, csv_path = emit_outputs(
        REPO_ROOT / "backend" / "output",
        args.snapshot_date,
        args.war_source,
        top,
        args.top,
    )

    print(f"Wrote {len(top)} players to {json_path} and {csv_path}")


if __name__ == "__main__":
    main()
