from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

SERVICE_DAYS_PER_YEAR = 172


@dataclass(frozen=True)
class ServiceTimeRecord:
    mlbam_id: int
    service_time_years: int
    service_time_days: int
    service_time_label: str | None = None

    @property
    def total_service_days(self) -> int:
        return self.service_time_years * SERVICE_DAYS_PER_YEAR + self.service_time_days


@dataclass(frozen=True)
class SuperTwoResult:
    cutoff_days: int
    eligible_ids: set[int]
    super_two_ids: set[int]


@dataclass(frozen=True)
class ControlYear:
    season_offset: int
    year_type: str  # prearb, arb1, arb2, arb3, arb4


@dataclass(frozen=True)
class ControlTimeline:
    years: list[ControlYear]

    @property
    def team_control_years_remaining(self) -> int:
        return len(self.years)


@dataclass(frozen=True)
class SeasonWindow:
    start: date
    end: date

    def is_in_season(self, snapshot: date) -> bool:
        return self.start <= snapshot <= self.end


def remaining_games_fraction(snapshot: date, season_window: SeasonWindow) -> float:
    if not season_window.is_in_season(snapshot):
        return 1.0
    season_days = (season_window.end - season_window.start).days + 1
    remaining_days = (season_window.end - snapshot).days + 1
    if season_days <= 0:
        return 1.0
    return max(0.0, min(1.0, remaining_days / season_days))


def compute_super_two(
    records: Iterable[ServiceTimeRecord],
    prior_season_days: dict[int, int] | None = None,
    eligibility_min_days: int = 86,
    top_pct: float = 0.22,
) -> SuperTwoResult:
    prior_season_days = prior_season_days or {}
    eligible: list[tuple[int, int]] = []

    for record in records:
        total_years = record.service_time_years
        if total_years < 2 or total_years >= 3:
            continue
        days_last = prior_season_days.get(record.mlbam_id)
        if days_last is None:
            days_last = record.service_time_days
        if days_last < eligibility_min_days:
            continue
        eligible.append((record.mlbam_id, days_last))

    if not eligible:
        return SuperTwoResult(cutoff_days=0, eligible_ids=set(), super_two_ids=set())

    eligible_sorted = sorted(eligible, key=lambda x: x[1], reverse=True)
    eligible_ids = {pid for pid, _ in eligible_sorted}
    cutoff_index = max(0, int(len(eligible_sorted) * top_pct) - 1)
    cutoff_days = eligible_sorted[cutoff_index][1]
    super_two_ids = {pid for pid, days in eligible_sorted if days >= cutoff_days}
    return SuperTwoResult(
        cutoff_days=cutoff_days,
        eligible_ids=eligible_ids,
        super_two_ids=super_two_ids,
    )


def super_two_for_snapshot(
    records: Iterable[ServiceTimeRecord],
    snapshot: date,
    season_window: SeasonWindow,
    prior_season_days: dict[int, int] | None = None,
) -> SuperTwoResult:
    # In-season snapshots use the most recent offseason determination.
    # Service time data is assumed to be from the last offseason.
    return compute_super_two(records, prior_season_days=prior_season_days)


def control_timeline(
    service_days_total: int, super_two: bool
) -> ControlTimeline:
    full_years = service_days_total // SERVICE_DAYS_PER_YEAR
    if full_years >= 6:
        return ControlTimeline([])

    service_year = full_years + 1
    years: list[ControlYear] = []
    season_offset = 0
    while service_year <= 6:
        year_type = "prearb"
        if service_year <= 2:
            year_type = "prearb"
        elif service_year == 3:
            year_type = "arb1" if super_two else "prearb"
        elif service_year == 4:
            year_type = "arb2" if super_two else "arb1"
        elif service_year == 5:
            year_type = "arb3" if super_two else "arb2"
        elif service_year == 6:
            year_type = "arb4" if super_two else "arb3"

        years.append(ControlYear(season_offset=season_offset, year_type=year_type))
        service_year += 1
        season_offset += 1

    return ControlTimeline(years)
