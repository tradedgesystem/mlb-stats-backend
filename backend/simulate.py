from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable

from backend.contracts import ContractYear
from backend.durability import DurabilityMixture
from backend.projections import AgingCurve


@dataclass(frozen=True)
class SimulationConfig:
    sims: int
    year_shock_sd: float
    talent_sd: float


@dataclass(frozen=True)
class SimulationInputs:
    rate_post: float
    usage_post: float
    age: int
    denom: float
    aging: AgingCurve
    horizon_years: int
    war_price_by_year: list[float]
    discount_rate: float
    contract_years: list[ContractYear]
    durability: DurabilityMixture
    in_season_fraction: float
    role_prob_sp: float | None = None


@dataclass(frozen=True)
class SimulationResult:
    samples: list[float]
    quantiles: dict[str, float]
    war_p50: list[float]
    mean: float
    std: float


def compute_quantiles(samples: list[float], qs: Iterable[float]) -> dict[str, float]:
    if not samples:
        return {f"p{int(q * 100)}": 0.0 for q in qs}
    samples_sorted = sorted(samples)
    n = len(samples_sorted)
    out = {}
    for q in qs:
        idx = int(round((n - 1) * q))
        idx = max(0, min(n - 1, idx))
        out[f"p{int(q * 100)}"] = samples_sorted[idx]
    return out


def select_role(role_prob_sp: float | None) -> str:
    if role_prob_sp is None:
        return "SP"
    roll = random.random()
    return "SP" if roll <= role_prob_sp else "RP"


def apply_option_decision(
    option_type: str | None,
    value_m: float,
    salary_m: float,
    buyout_m: float | None,
    market_m: float,
) -> tuple[bool, float]:
    if option_type is None:
        return True, salary_m
    buyout = buyout_m or 0.0
    if option_type == "CO":
        exercised = (value_m - salary_m) > -buyout
        return exercised, salary_m if exercised else buyout
    if option_type == "PO":
        exercised = salary_m > market_m
        return exercised, salary_m if exercised else buyout
    if option_type == "MO":
        club = (value_m - salary_m) > -buyout
        player = salary_m > market_m
        exercised = club and player
        return exercised, salary_m if exercised else buyout
    return True, salary_m


def simulate_tvp(
    config: SimulationConfig,
    inputs: SimulationInputs,
    expected_war: list[float],
) -> SimulationResult:
    samples: list[float] = []
    random.seed(42)

    for _ in range(config.sims):
        talent_rate = random.gauss(inputs.rate_post, config.talent_sd)
        role = select_role(inputs.role_prob_sp)
        tvp = 0.0
        active = True

        for t in range(inputs.horizon_years):
            if not active:
                break
            age_t = inputs.age + t
            rate_t = talent_rate + random.gauss(0.0, config.year_shock_sd)
            rate_t *= inputs.aging.rate_multiplier(age_t)

            usage_base = inputs.usage_post * inputs.aging.usage_multiplier(age_t)
            if t == 0:
                usage_base *= inputs.in_season_fraction

            state_roll = random.random()
            cumulative = 0.0
            usage_mult = 0.0
            for state in inputs.durability.states:
                cumulative += state.probability
                if state_roll <= cumulative:
                    usage_mult = state.usage_multiplier
                    break
            usage_t = usage_base * usage_mult

            war_t = rate_t * (usage_t / inputs.denom) if inputs.denom else 0.0

            price_t = inputs.war_price_by_year[t]
            value_t = war_t * price_t
            market_t = expected_war[t] * price_t if t < len(expected_war) else 0.0

            cost_entry = inputs.contract_years[t]
            cost_t = cost_entry.cost_m
            if t == 0:
                cost_t *= inputs.in_season_fraction

            if cost_entry.option_type:
                exercised, cost_t = apply_option_decision(
                    cost_entry.option_type,
                    value_t,
                    cost_entry.option_salary_m or cost_t,
                    cost_entry.option_buyout_m,
                    market_t,
                )
                if not exercised:
                    active = False

            surplus_t = value_t - cost_t
            disc = 1.0 / ((1.0 + inputs.discount_rate) ** t)
            tvp += surplus_t * disc

        samples.append(tvp)

    quantiles = compute_quantiles(samples, [0.1, 0.5, 0.9])
    mean = sum(samples) / len(samples) if samples else 0.0
    variance = sum((val - mean) ** 2 for val in samples) / len(samples) if samples else 0.0
    std = math.sqrt(variance)
    return SimulationResult(samples=samples, quantiles=quantiles, war_p50=expected_war, mean=mean, std=std)
