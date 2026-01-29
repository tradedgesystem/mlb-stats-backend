from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class DurabilityState:
    label: str
    probability: float
    usage_multiplier: float


@dataclass(frozen=True)
class DurabilityMixture:
    states: list[DurabilityState]

    def normalize(self) -> "DurabilityMixture":
        total = sum(state.probability for state in self.states)
        if total <= 0:
            return self
        normalized = [
            DurabilityState(state.label, state.probability / total, state.usage_multiplier)
            for state in self.states
        ]
        return DurabilityMixture(normalized)


@dataclass(frozen=True)
class DurabilityConfig:
    full: float
    partial: float
    lost: float
    partial_multiplier: float
    lost_multiplier: float
    age_risk_per_year: float
    workload_spike_penalty: float


@dataclass(frozen=True)
class DurabilityInputs:
    is_pitcher: bool
    age: int
    workload_spike: bool = False
    il_history_years: int = 0


def build_mixture(inputs: DurabilityInputs, cfg_hit: DurabilityConfig, cfg_pit: DurabilityConfig) -> DurabilityMixture:
    cfg = cfg_pit if inputs.is_pitcher else cfg_hit
    age_delta = max(0, inputs.age - 27)
    age_penalty = age_delta * cfg.age_risk_per_year
    workload_penalty = cfg.workload_spike_penalty if inputs.workload_spike else 0.0
    il_penalty = min(0.2, inputs.il_history_years * 0.02)

    full = max(0.0, cfg.full - age_penalty - workload_penalty - il_penalty)
    partial = max(0.0, cfg.partial + age_penalty * 0.5)
    lost = max(0.0, cfg.lost + age_penalty * 0.5 + workload_penalty + il_penalty)

    mixture = DurabilityMixture(
        [
            DurabilityState("full", full, 1.0),
            DurabilityState("partial", partial, cfg.partial_multiplier),
            DurabilityState("lost", lost, cfg.lost_multiplier),
        ]
    )
    return mixture.normalize()
