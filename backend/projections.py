from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class AgingCurve:
    peak_age: int
    rate_delta_before: float
    rate_delta_after: float
    usage_delta_before: float
    usage_delta_after: float

    def rate_multiplier(self, age: int) -> float:
        delta = age - self.peak_age
        if delta < 0:
            return max(0.0, 1.0 + delta * self.rate_delta_before)
        return max(0.0, 1.0 + delta * self.rate_delta_after)

    def usage_multiplier(self, age: int) -> float:
        delta = age - self.peak_age
        if delta < 0:
            return max(0.0, 1.0 + delta * self.usage_delta_before)
        return max(0.0, 1.0 + delta * self.usage_delta_after)


@dataclass(frozen=True)
class SeasonHistory:
    season: int
    war: float
    usage: float


@dataclass(frozen=True)
class RateProjection:
    rate_obs: float
    usage_obs: float
    n_usage: float
    rate_post: float
    usage_post: float


MARCE_L_WEIGHTS = (3.0, 4.0, 5.0)


def weighted_rate(history: Iterable[SeasonHistory], denom: float) -> tuple[float, float]:
    seasons = list(history)
    if not seasons:
        return 0.0, 0.0
    weights = MARCE_L_WEIGHTS[-len(seasons):]
    weighted_war = 0.0
    weighted_usage = 0.0
    usage_total = 0.0
    for weight, entry in zip(weights, seasons):
        if entry.usage <= 0:
            continue
        weighted_war += weight * entry.war
        weighted_usage += weight * entry.usage
        usage_total += entry.usage
    if weighted_usage <= 0:
        return 0.0, usage_total
    rate_obs = weighted_war / (weighted_usage / denom)
    return rate_obs, usage_total


def weighted_usage(history: Iterable[SeasonHistory]) -> float:
    seasons = list(history)
    if not seasons:
        return 0.0
    weights = MARCE_L_WEIGHTS[-len(seasons):]
    total_weight = 0.0
    usage_sum = 0.0
    for weight, entry in zip(weights, seasons):
        usage_sum += weight * entry.usage
        total_weight += weight
    if total_weight <= 0:
        return 0.0
    return usage_sum / total_weight


def regress_rate(rate_obs: float, n: float, rate_prior: float, k_rate: float) -> float:
    denom = n + k_rate
    if denom <= 0:
        return rate_prior
    return (n * rate_obs + k_rate * rate_prior) / denom


def regress_usage(usage_obs: float, n: float, usage_prior: float, k_u: float) -> float:
    denom = n + k_u
    if denom <= 0:
        return usage_prior
    return (n * usage_obs + k_u * usage_prior) / denom


def build_rate_projection(
    history: Iterable[SeasonHistory],
    denom: float,
    rate_prior: float,
    k_rate: float,
    usage_prior: float,
    k_u: float,
) -> RateProjection:
    rate_obs, n_usage = weighted_rate(history, denom)
    usage_obs = weighted_usage(history)
    rate_post = regress_rate(rate_obs, n_usage, rate_prior, k_rate)
    usage_post = regress_usage(usage_obs, n_usage, usage_prior, k_u)
    return RateProjection(
        rate_obs=rate_obs,
        usage_obs=usage_obs,
        n_usage=n_usage,
        rate_post=rate_post,
        usage_post=usage_post,
    )


def expected_war_path(
    rate_post: float,
    usage_post: float,
    age: int,
    years: int,
    denom: float,
    aging: AgingCurve,
) -> list[float]:
    wars: list[float] = []
    for t in range(years):
        age_t = age + t
        rate_t = rate_post * aging.rate_multiplier(age_t)
        usage_t = usage_post * aging.usage_multiplier(age_t)
        war_t = rate_t * (usage_t / denom) if denom else 0.0
        wars.append(war_t)
    return wars
