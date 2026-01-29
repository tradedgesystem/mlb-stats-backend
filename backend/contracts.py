from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContractYear:
    season: int
    cost_m: float
    basis: str
    option_type: str | None = None
    option_salary_m: float | None = None
    option_buyout_m: float | None = None


@dataclass(frozen=True)
class ContractSchedule:
    years: list[ContractYear]

    def by_season_offset(self, snapshot_year: int) -> list[ContractYear]:
        return sorted(self.years, key=lambda y: y.season)


def min_salary_for_year(index: int, min_salary_m: float, growth: float) -> float:
    return min_salary_m * ((1.0 + growth) ** index)


def arb_cost(
    war_expected: float,
    war_price: float,
    arb_share: float,
) -> float:
    return war_expected * war_price * arb_share


def build_guaranteed_schedule(
    contract: dict[str, Any],
    snapshot_year: int,
) -> dict[int, float]:
    guaranteed: dict[int, float] = {}
    for year in contract.get("contract_years") or []:
        season = year.get("season")
        salary = year.get("salary_m")
        if season is None or season < snapshot_year:
            continue
        if salary is None:
            salary = 0.0
        guaranteed[int(season)] = float(salary)
    if not guaranteed:
        total_value = contract.get("total_value_m")
        years = contract.get("years_remaining")
        if total_value is not None and years:
            try:
                aav = float(total_value) / float(years)
            except (TypeError, ValueError, ZeroDivisionError):
                aav = None
            if aav is not None:
                for i in range(int(years)):
                    season = snapshot_year + i
                    guaranteed[season] = aav
    return guaranteed


def build_option_schedule(contract: dict[str, Any]) -> dict[int, dict[str, Any]]:
    options = {}
    for option in contract.get("options") or []:
        season = option.get("season")
        if season is None:
            continue
        options[int(season)] = {
            "type": option.get("type"),
            "salary_m": option.get("salary_m"),
            "buyout_m": option.get("buyout_m"),
        }
    return options


def build_contract_schedule(
    contract: dict[str, Any],
    snapshot_year: int,
    horizon_years: int,
    control_year_types: list[str],
    expected_war: list[float],
    war_price_by_year: list[float],
    arb_share: list[float],
    min_salary_m: float,
    min_salary_growth: float,
) -> ContractSchedule:
    guaranteed = build_guaranteed_schedule(contract, snapshot_year)
    options = build_option_schedule(contract)
    years: list[ContractYear] = []

    for t in range(horizon_years):
        season = snapshot_year + t
        if season in guaranteed:
            years.append(ContractYear(season, float(guaranteed[season]), "guaranteed"))
            continue
        if season in options:
            option = options[season]
            salary = option.get("salary_m")
            years.append(
                ContractYear(
                    season=season,
                    cost_m=float(salary) if salary is not None else 0.0,
                    basis="option",
                    option_type=option.get("type"),
                    option_salary_m=salary,
                    option_buyout_m=option.get("buyout_m"),
                )
            )
            continue
        if t < len(control_year_types):
            year_type = control_year_types[t]
            if year_type == "prearb":
                cost = min_salary_for_year(t, min_salary_m, min_salary_growth)
                years.append(ContractYear(season, cost, "model_cost_prearb"))
            else:
                arb_index = int(year_type.replace("arb", "")) - 1
                share = arb_share[arb_index] if arb_index < len(arb_share) else arb_share[-1]
                war_exp = expected_war[t] if t < len(expected_war) else 0.0
                price = war_price_by_year[t] if t < len(war_price_by_year) else 0.0
                cost = arb_cost(war_exp, price, share)
                years.append(ContractYear(season, cost, "model_cost_arb"))
        else:
            years.append(ContractYear(season, 0.0, "fa"))

    return ContractSchedule(years)
