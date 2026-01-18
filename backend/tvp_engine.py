from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import exp
from pathlib import Path
import json
import re
from typing import Any, Iterable


@dataclass(frozen=True)
class TvpConfig:
    snapshot_year: int
    dollars_per_war: float
    war_price_growth: float
    discount_rate: float
    min_salary_m: float
    min_salary_growth: float
    arb_share: list[float]
    role_mult: float
    star_mult: float
    eta_survive_base: float
    option_sigmoid_k: float
    fa_share: float
    min_fa_m: float
    package_step_mults: list[float]
    package_power_p: float
    cash_value_cap_pct: float
    cash_value_haircut: float
    prospect_floor: bool
    top100_ranges: list[tuple[int, int, float]]
    position_mult: dict[str, float]
    war6_base: dict[int, float]
    fv_probabilities: dict[int, dict[str, float]]
    war_profile_weights: list[float]
    pitcher_profile_weights: list[float]
    fv_war_rate_prior: dict[int, float]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "TvpConfig":
        top100_ranges = _parse_range_map(data.get("top100_mult", {}))
        position_mult = {
            k.upper(): float(v) for k, v in data.get("position_mult", {}).items()
        }
        war6_base = _parse_int_float_map(data.get("war6_base", {}))
        fv_probabilities = _parse_probability_map(data.get("fv_probabilities", {}))
        fv_war_rate_prior = _parse_int_float_map(data.get("fv_war_rate_prior", {}))
        return TvpConfig(
            snapshot_year=int(data["snapshot_year"]),
            dollars_per_war=float(data["dollars_per_war"]),
            war_price_growth=float(data.get("war_price_growth", 0.0)),
            discount_rate=float(data["discount_rate"]),
            min_salary_m=float(data["min_salary_m"]),
            min_salary_growth=float(data.get("min_salary_growth", 0.0)),
            arb_share=[float(v) for v in data.get("arb_share", [])],
            role_mult=float(data["role_mult"]),
            star_mult=float(data["star_mult"]),
            eta_survive_base=float(data["eta_survive_base"]),
            option_sigmoid_k=float(data["option_sigmoid_k"]),
            fa_share=float(data["fa_share"]),
            min_fa_m=float(data["min_fa_m"]),
            package_step_mults=[float(v) for v in data.get("package_step_mults", [])],
            package_power_p=float(data.get("package_power_p", 0.92)),
            cash_value_cap_pct=float(data.get("cash_value_cap_pct", 0.0)),
            cash_value_haircut=float(data.get("cash_value_haircut", 1.0)),
            prospect_floor=bool(data.get("prospect_floor", True)),
            top100_ranges=top100_ranges,
            position_mult=position_mult,
            war6_base=war6_base,
            fv_probabilities=fv_probabilities,
            war_profile_weights=[float(v) for v in data.get("war_profile_weights", [])],
            pitcher_profile_weights=[
                float(v) for v in data.get("pitcher_profile_weights", [])
            ],
            fv_war_rate_prior=fv_war_rate_prior,
        )


def load_config(path: Path | None = None) -> TvpConfig:
    if path is None:
        path = Path(__file__).with_name("tvp_config.json")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return TvpConfig.from_dict(data)


def discount_factor(t: int, discount_rate: float) -> float:
    return 1.0 / ((1.0 + discount_rate) ** t)


def war_price(config: TvpConfig, t: int) -> float:
    return config.dollars_per_war * ((1.0 + config.war_price_growth) ** t)


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + exp(-x))


def parse_fv_value(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    digits = re.findall(r"\d+", str(raw))
    return int(digits[0]) if digits else None


def parse_eta_year(raw: Any, snapshot_year: int) -> int:
    if raw is None:
        return snapshot_year
    if isinstance(raw, (int, float)):
        return int(raw)
    digits = re.findall(r"\d{4}", str(raw))
    if digits:
        return int(digits[0])
    digits = re.findall(r"\d+", str(raw))
    return int(digits[0]) if digits else snapshot_year


def top100_multiplier(rank: int | None, config: TvpConfig) -> float:
    if rank is None:
        return 1.0
    for lower, upper, mult in config.top100_ranges:
        if lower <= rank <= upper:
            return mult
    return 1.0


def is_pitcher_position(position: str | None) -> bool:
    tokens = position_tokens(position)
    return any(token in {"SP", "RP", "P", "RHP", "LHP"} for token in tokens)


def position_tokens(position: str | None) -> list[str]:
    if not position:
        return []
    raw = re.split(r"[\\s,/\\-]+", position.upper())
    return [token for token in raw if token]


def position_multiplier(position: str | None, config: TvpConfig) -> float:
    tokens = position_tokens(position)
    if not tokens:
        return 1.0
    multipliers = []
    for token in tokens:
        if token in {"RHP", "LHP", "P"}:
            token = "SP"
        multipliers.append(config.position_mult.get(token))
    multipliers = [value for value in multipliers if value is not None]
    return max(multipliers) if multipliers else 1.0


def compute_prospect_tvp(prospect: dict[str, Any], config: TvpConfig) -> dict[str, Any]:
    fv_value = parse_fv_value(prospect.get("fv_value"))
    if fv_value is None or fv_value not in config.war6_base:
        return _prospect_error_result(prospect, config, "missing_fv_value")

    eta_year = parse_eta_year(prospect.get("eta"), config.snapshot_year)
    years_to_mlb = max(0, eta_year - config.snapshot_year)

    war6_base = config.war6_base[fv_value]
    pitcher = is_pitcher_position(prospect.get("position"))
    weights = (
        config.pitcher_profile_weights
        if pitcher and config.pitcher_profile_weights
        else config.war_profile_weights
    )
    war_base = [war6_base * weight for weight in weights]

    top100_mult = top100_multiplier(prospect.get("top_100_rank"), config)
    war_adj = [war * top100_mult for war in war_base]

    prob_row = config.fv_probabilities.get(fv_value)
    if not prob_row:
        return _prospect_error_result(prospect, config, "missing_fv_probabilities")

    p_bust = float(prob_row["p_bust"])
    p_role = float(prob_row["p_role"])
    p_star = float(prob_row["p_star"])

    if pitcher:
        p_bust, p_star = _shift_probability(p_bust, p_star, 0.05)
    if prospect.get("age") is not None and prospect.get("age") <= 19:
        p_bust, p_star = _shift_probability(p_bust, p_star, 0.03)

    p_survive = config.eta_survive_base**years_to_mlb
    p_bust = 1.0 - p_survive * (1.0 - p_bust)
    remaining = max(0.0, 1.0 - p_bust)
    prior_remaining = max(1e-9, p_role + p_star)
    p_role = remaining * (p_role / prior_remaining)
    p_star = remaining * (p_star / prior_remaining)

    war_expected = []
    for war in war_adj:
        expected = (p_role * config.role_mult + p_star * config.star_mult) * war
        war_expected.append(expected)

    pos_mult = position_multiplier(prospect.get("position"), config)
    war_expected = [war * pos_mult for war in war_expected]

    fwar_by_year = []
    price_by_year = []
    value_by_year = []
    salary_by_year = []
    surplus_by_year = []
    discount_factors = []
    pv_surplus_by_year = []

    for idx, war in enumerate(war_expected, start=1):
        t = years_to_mlb + (idx - 1)
        price = war_price(config, t)
        value = war * price
        salary_min = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
        arb_share = (
            config.arb_share[idx - 1] if idx - 1 < len(config.arb_share) else 0.0
        )
        salary = max(salary_min, arb_share * value)
        surplus = value - salary
        disc = discount_factor(t, config.discount_rate)
        pv = surplus * disc

        value_by_year.append(value)
        salary_by_year.append(salary)
        surplus_by_year.append(surplus)
        discount_factors.append(disc)
        pv_surplus_by_year.append(pv)

    tvp_raw = sum(pv_surplus_by_year)
    org_bonus = _org_rank_bonus(
        prospect.get("system_rank"), prospect.get("top_100_rank")
    )
    tvp = tvp_raw + org_bonus
    if config.prospect_floor:
        tvp = max(0.0, tvp)

    timestamp = _now_timestamp()
    return {
        "mlb_id": prospect.get("mlb_id"),
        "player_name": prospect.get("player_name"),
        "age": prospect.get("age"),
        "status": "prospect",
        "tvp_prospect": tvp,
        "tvp_mlb": None,
        "tvp_current": tvp,
        "raw_components": {
            "fv_value": fv_value,
            "eta_year": eta_year,
            "years_to_mlb": years_to_mlb,
            "top100_mult": top100_mult,
            "p_bust": p_bust,
            "p_role": p_role,
            "p_star": p_star,
            "role_mult": config.role_mult,
            "star_mult": config.star_mult,
            "pos_mult": pos_mult,
            "war_profile": war_expected,
            "value_by_year": value_by_year,
            "salary_by_year": salary_by_year,
            "surplus_by_year": surplus_by_year,
            "discount_factors": discount_factors,
            "pv_surplus_by_year": pv_surplus_by_year,
            "org_bonus": org_bonus,
            "tvp_prospect_raw": tvp_raw,
        },
        "snapshot_year": config.snapshot_year,
        "last_updated_timestamp": timestamp,
    }


def compute_mlb_tvp(
    fwar_by_year: list[float],
    salary_by_year_m: list[float],
    config: TvpConfig,
    option_years: Iterable[dict[str, Any]] | None = None,
    rel_mult: float = 1.0,
    current_year_fraction: float | None = None,
    salary_retained_by_year: list[float] | None = None,
    cash_sent_by_year: list[float] | None = None,
) -> dict[str, Any]:
    max_len = max(len(fwar_by_year), len(salary_by_year_m))
    fwar = list(fwar_by_year) + [0.0] * (max_len - len(fwar_by_year))
    salary = list(salary_by_year_m) + [0.0] * (max_len - len(salary_by_year_m))
    salary_retained = list(salary_retained_by_year or []) + [0.0] * (
        max_len - len(salary_retained_by_year or [])
    )
    cash_sent = list(cash_sent_by_year or []) + [0.0] * (
        max_len - len(cash_sent_by_year or [])
    )

    fwar_by_year = []
    price_by_year = []
    value_by_year = []
    salary_by_year = []
    surplus_by_year = []
    discount_factors = []
    pv_surplus_by_year = []

    for t, war in enumerate(fwar):
        price = war_price(config, t)
        value = war * price
        salary_t = salary[t] - salary_retained[t] + cash_sent[t]
        if current_year_fraction is not None and t == 0:
            value *= current_year_fraction
            salary_t *= current_year_fraction
        surplus = value - salary_t
        disc = discount_factor(t, config.discount_rate)
        pv = surplus * disc

        fwar_by_year.append(war)
        price_by_year.append(price)
        value_by_year.append(value)
        salary_by_year.append(salary_t)
        surplus_by_year.append(surplus)
        discount_factors.append(disc)
        pv_surplus_by_year.append(pv)

    option_details = []
    option_total = 0.0
    for option in option_years or []:
        ev, detail = compute_option_ev(option, fwar, config)
        option_total += ev
        option_details.append(detail)

    tvp_base = sum(pv_surplus_by_year) + option_total
    tvp_mlb = tvp_base * rel_mult

    return {
        "tvp_mlb": tvp_mlb,
        "tvp_mlb_base": tvp_base,
        "raw_components": {
            "fwar_by_year": fwar_by_year,
            "price_by_year": price_by_year,
            "value_by_year": value_by_year,
            "salary_by_year_input": salary,
            "salary_retained_by_year": salary_retained,
            "cash_sent_by_year": cash_sent,
            "salary_by_year": salary_by_year,
            "surplus_by_year": surplus_by_year,
            "discount_factors": discount_factors,
            "pv_surplus_by_year": pv_surplus_by_year,
            "rel_mult": rel_mult,
            "options": option_details,
        },
    }


def compute_option_ev(
    option: dict[str, Any], fwar_by_year: list[float], config: TvpConfig
) -> tuple[float, dict[str, Any]]:
    option_type = str(option.get("option_type", "")).upper()
    t = int(option.get("t", 0))
    salary = float(option.get("option_salary_m", 0.0))
    buyout = float(option.get("buyout_m", 0.0))
    fwar = float(option.get("fwar", fwar_by_year[t] if t < len(fwar_by_year) else 0.0))

    price = war_price(config, t)
    value = fwar * price
    market = None
    sigmoid_input_ex = None
    sigmoid_input_in = None
    p_ex = None
    p_in = None
    p_team = None
    p_player = None
    if option_type == "CO":
        sigmoid_input_ex = ((value - salary) + buyout) / config.option_sigmoid_k
        p_ex = sigmoid(sigmoid_input_ex)
        ev = p_ex * (value - salary) + (1.0 - p_ex) * (-buyout)
    elif option_type == "PO":
        market = max(config.min_fa_m, config.fa_share * value)
        sigmoid_input_in = (salary - market) / config.option_sigmoid_k
        p_in = sigmoid(sigmoid_input_in)
        ev = p_in * (value - salary)
    elif option_type == "MO":
        market = max(config.min_fa_m, config.fa_share * value)
        sigmoid_input_ex = ((value - salary) + buyout) / config.option_sigmoid_k
        sigmoid_input_in = (salary - market) / config.option_sigmoid_k
        p_team = sigmoid(sigmoid_input_ex)
        p_player = sigmoid(sigmoid_input_in)
        p_ex = p_team * p_player
        p_in = p_player
        ev = p_ex * (value - salary) + (1.0 - p_ex) * (-buyout)
    else:
        ev = 0.0

    pv = ev * discount_factor(t, config.discount_rate)
    detail = {
        "t": t,
        "option_type": option_type,
        "fwar_used_for_option": fwar,
        "V": value,
        "S": salary,
        "B": buyout,
        "market": market,
        "sigmoid_input_exercise": sigmoid_input_ex,
        "sigmoid_input_in": sigmoid_input_in,
        "probabilities": {
            "P_ex": p_ex,
            "P_in": p_in,
            "P_team": p_team,
            "P_player": p_player,
        },
        "ev": ev,
        "pv_ev": pv,
    }
    return pv, detail


def compute_rookie_alpha(
    fv_value: int,
    is_pitcher: bool,
    pa: float | None,
    ip: float | None,
    fwar_to_date: float,
    config: TvpConfig,
) -> dict[str, float]:
    prior_rate = config.fv_war_rate_prior.get(fv_value, 0.0)
    if is_pitcher:
        ip_value = ip or 0.0
        expected = prior_rate * (ip_value / 180.0) if ip_value else 0.0
        delta = fwar_to_date - expected
        evidence = min(1.0, abs(delta) / 1.5) * min(1.0, ip_value / 75.0)
        if ip_value < 70:
            alpha_base = 0.85
        elif ip_value < 150:
            alpha_base = 0.70
        elif ip_value < 300:
            alpha_base = 0.55
        elif ip_value < 500:
            alpha_base = 0.40
        else:
            alpha_base = 0.25
        if ip_value >= 250 and fwar_to_date <= -1.0:
            alpha_base = min(alpha_base, 0.10)
    else:
        pa_value = pa or 0.0
        expected = prior_rate * (pa_value / 600.0) if pa_value else 0.0
        delta = fwar_to_date - expected
        evidence = min(1.0, abs(delta) / 1.5) * min(1.0, pa_value / 300.0)
        if pa_value < 200:
            alpha_base = 0.85
        elif pa_value < 600:
            alpha_base = 0.70
        elif pa_value < 1200:
            alpha_base = 0.55
        elif pa_value < 2000:
            alpha_base = 0.40
        else:
            alpha_base = 0.25
        if pa_value >= 800 and fwar_to_date <= -1.0:
            alpha_base = min(alpha_base, 0.10)

    alpha = alpha_base * (1.0 - 0.35 * evidence)
    return {
        "alpha": alpha,
        "alpha_base": alpha_base,
        "evidence": evidence,
        "prior_rate": prior_rate,
    }


def update_war_rate(
    fv_value: int,
    is_pitcher: bool,
    pa: float | None,
    ip: float | None,
    fwar_to_date: float,
    config: TvpConfig,
    evidence: float,
) -> float:
    prior_rate = config.fv_war_rate_prior.get(fv_value, 0.0)
    if is_pitcher:
        ip_value = ip or 0.0
        w_base = ip_value / (ip_value + 150.0) if ip_value else 0.0
        war_rate_obs = fwar_to_date / (ip_value / 180.0) if ip_value else 0.0
    else:
        pa_value = pa or 0.0
        w_base = pa_value / (pa_value + 600.0) if pa_value else 0.0
        war_rate_obs = fwar_to_date / (pa_value / 600.0) if pa_value else 0.0
    w_shock = 0.25 * evidence
    w = min(0.85, max(0.0, w_base + w_shock))
    return (1.0 - w) * prior_rate + w * war_rate_obs


def apply_step_package_rule(values: list[float], config: TvpConfig) -> float:
    multipliers = config.package_step_mults or [1.0]
    total = 0.0
    for idx, value in enumerate(sorted(values, reverse=True)):
        mult = multipliers[idx] if idx < len(multipliers) else multipliers[-1]
        total += value * mult
    return total


def apply_power_package_rule(values: list[float], config: TvpConfig) -> float:
    return sum(value**config.package_power_p for value in values)


def cap_cash_value(
    non_cash_tvp: float, cash_tvp: float, config: TvpConfig
) -> tuple[float, float]:
    cash_tvp_adj = cash_tvp * config.cash_value_haircut
    cap_pct = config.cash_value_cap_pct
    if non_cash_tvp <= 0:
        return cash_tvp_adj, non_cash_tvp + cash_tvp_adj
    if cap_pct <= 0:
        return cash_tvp_adj, non_cash_tvp + cash_tvp_adj
    cap_value = cap_pct * non_cash_tvp
    cash_capped = min(cash_tvp_adj, cap_value)
    return cash_capped, non_cash_tvp + cash_capped


def compute_package_tvp(
    asset_tvps: list[float],
    cash_tvp: float,
    config: TvpConfig,
    method: str = "step",
) -> dict[str, float]:
    if method == "power":
        non_cash = apply_power_package_rule(asset_tvps, config)
    else:
        non_cash = apply_step_package_rule(asset_tvps, config)
    cash_capped, total = cap_cash_value(non_cash, cash_tvp, config)
    return {
        "non_cash_tvp": non_cash,
        "cash_tvp_raw": cash_tvp,
        "cash_tvp_capped": cash_capped,
        "package_tvp": total,
    }


def _parse_range_map(raw: dict[str, float]) -> list[tuple[int, int, float]]:
    ranges: list[tuple[int, int, float]] = []
    for key, value in raw.items():
        if "-" in key:
            parts = key.split("-", 1)
            lower = int(parts[0])
            upper = int(parts[1])
        else:
            lower = upper = int(key)
        ranges.append((lower, upper, float(value)))
    ranges.sort()
    return ranges


def _parse_int_float_map(raw: dict[str, Any]) -> dict[int, float]:
    return {int(key): float(value) for key, value in raw.items()}


def _parse_probability_map(raw: dict[str, Any]) -> dict[int, dict[str, float]]:
    result: dict[int, dict[str, float]] = {}
    for key, value in raw.items():
        result[int(key)] = {
            "p_bust": float(value["p_bust"]),
            "p_role": float(value["p_role"]),
            "p_star": float(value["p_star"]),
        }
    return result


def _shift_probability(
    p_bust: float, p_star: float, shift: float
) -> tuple[float, float]:
    shift = min(shift, p_star)
    return p_bust + shift, p_star - shift


def _org_rank_bonus(system_rank: int | None, top_100_rank: int | None) -> float:
    if top_100_rank is not None or system_rank is None:
        return 0.0
    if system_rank <= 5:
        return 2.0
    if system_rank <= 10:
        return 1.0
    if system_rank <= 20:
        return 0.5
    return 0.0


def _prospect_error_result(
    prospect: dict[str, Any], config: TvpConfig, error: str
) -> dict[str, Any]:
    return {
        "mlb_id": prospect.get("mlb_id"),
        "player_name": prospect.get("player_name"),
        "age": prospect.get("age"),
        "status": "prospect",
        "tvp_prospect": None,
        "tvp_mlb": None,
        "tvp_current": None,
        "raw_components": {"error": error},
        "snapshot_year": config.snapshot_year,
        "last_updated_timestamp": _now_timestamp(),
    }


def _now_timestamp() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
