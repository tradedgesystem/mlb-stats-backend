import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from backend.contracts import build_contract_schedule
from backend.service_time import ControlTimeline
from backend.output import PlayerOutput, emit_outputs, build_breakdown, emit_ranked_outputs
from backend.projections import SeasonHistory, build_rate_projection
from backend.service_time import ServiceTimeRecord, SeasonWindow, compute_super_two, super_two_for_snapshot
from backend.simulate import SimulationConfig, SimulationInputs, apply_option_decision, compute_quantiles, simulate_tvp
from backend.durability import DurabilityState, DurabilityMixture
from backend.projections import AgingCurve
from backend.contracts import ContractYear
from backend.compute_mlb_tvp import (
    is_player_eligible,
    total_usage,
    usage_prior_for_player,
    load_config,
    seasons_with_usage,
    apply_contract_overrides,
    should_use_aav_for_deferrals,
    resolve_projection_role,
    determine_role,
    build_player_output,
    leaderboard_eligible,
    risk_adjusted_value,
    build_status_t,
    late_negative_surplus_years,
    backloaded_contract,
    is_prospect_like,
)


def test_super_two_top22_with_ties():
    records = [
        ServiceTimeRecord(mlbam_id=i, service_time_years=2, service_time_days=days)
        for i, days in enumerate([120, 118, 115, 110, 110, 109, 90, 88, 87, 86], start=1)
    ]
    result = compute_super_two(records)
    assert result.cutoff_days == 118
    assert result.super_two_ids == {1, 2}


def test_super_two_in_season_uses_last_offseason():
    records = [ServiceTimeRecord(mlbam_id=1, service_time_years=2, service_time_days=120)]
    window = SeasonWindow(start=date(2026, 4, 1), end=date(2026, 10, 1))
    snapshot = date(2026, 6, 1)
    offseason = super_two_for_snapshot(records, snapshot, window)
    direct = compute_super_two(records)
    assert offseason.super_two_ids == direct.super_two_ids


def test_usage_shrinkage_and_prior_math():
    history = [SeasonHistory(season=2025, war=3.0, usage=600.0)]
    projection = build_rate_projection(
        history,
        denom=600.0,
        rate_prior=2.0,
        k_rate=600.0,
        usage_prior=300.0,
        k_u=300.0,
    )
    assert projection.rate_post == pytest.approx(2.5)
    assert projection.usage_post == pytest.approx(500.0)


def test_option_decisions_are_deterministic():
    exercised, cost = apply_option_decision("CO", value_m=10.0, salary_m=8.0, buyout_m=2.0, market_m=9.0)
    assert exercised is True
    assert cost == 8.0

    exercised, cost = apply_option_decision("CO", value_m=5.0, salary_m=8.0, buyout_m=2.0, market_m=9.0)
    assert exercised is False
    assert cost == 2.0

    exercised, cost = apply_option_decision("PO", value_m=10.0, salary_m=8.0, buyout_m=1.0, market_m=9.0)
    assert exercised is False
    assert cost == 1.0


def test_monte_carlo_quantiles_from_samples():
    durability = DurabilityMixture([
        DurabilityState("full", 1.0, 1.0),
    ])
    sim_cfg = SimulationConfig(sims=5, year_shock_sd=0.0, talent_sd=0.0)
    inputs = SimulationInputs(
        rate_post=2.0,
        usage_post=600.0,
        age=25,
        denom=600.0,
        aging=AgingCurve(peak_age=27, rate_delta_before=0.0, rate_delta_after=0.0, usage_delta_before=0.0, usage_delta_after=0.0),
        horizon_years=1,
        war_price_by_year=[10.0],
        discount_rate=0.1,
        contract_years=[ContractYear(season=2026, cost_m=5.0, basis="guaranteed")],
        durability=durability,
        in_season_fraction=1.0,
        role_prob_sp=None,
    )
    result = simulate_tvp(sim_cfg, inputs, expected_war=[2.0])
    expected = compute_quantiles(result.samples, [0.1, 0.5, 0.9])
    assert result.quantiles == expected


def test_arb_cost_uses_expected_war_not_sample():
    schedule = build_contract_schedule(
        contract={},
        snapshot_year=2026,
        horizon_years=1,
        control_year_types=["arb1"],
        expected_war=[2.0],
        war_price_by_year=[10.0],
        arb_share=[0.5],
        min_salary_m=1.0,
        min_salary_growth=0.0,
    )
    assert schedule.years[0].cost_m == pytest.approx(10.0)
    assert schedule.years[0].basis == "model_cost_arb"


def test_component_output_only_when_present(tmp_path: Path):
    player = PlayerOutput(
        mlbam_id=1,
        name="Test",
        team="TST",
        age=25,
        role="H",
        position="SS",
        status_t=["contract"],
        tvp=1.75,
        tvp_p10=1.0,
        tvp_p50=2.0,
        tvp_p90=3.0,
        talent_value_p50=2.5,
        tvp_mean=2.0,
        tvp_std=0.5,
        tvp_risk_adj=1.75,
        flags={},
        contract_source=None,
        contract_confidence=None,
        late_negative_surplus_years=0,
        breakdown=[],
        service_time="01/2026",
        components=None,
    )
    json_path, _ = emit_outputs(tmp_path, "2026-01-01", "bWAR", [player], 1)
    payload = json.loads(Path(json_path).read_text())
    assert "components" not in payload["players"][0]

    player_with_components = PlayerOutput(
        mlbam_id=2,
        name="Test2",
        team="TST",
        age=25,
        role="H",
        position="SS",
        status_t=["contract"],
        tvp=1.75,
        tvp_p10=1.0,
        tvp_p50=2.0,
        tvp_p90=3.0,
        talent_value_p50=2.5,
        tvp_mean=2.0,
        tvp_std=0.5,
        tvp_risk_adj=1.75,
        flags={},
        contract_source=None,
        contract_confidence=None,
        late_negative_surplus_years=0,
        breakdown=[],
        service_time="01/2026",
        components={"bat": 1.0},
    )
    json_path, _ = emit_outputs(tmp_path, "2026-01-01", "bWAR", [player_with_components], 1)
    payload = json.loads(Path(json_path).read_text())
    assert payload["players"][0]["components"] == {"bat": 1.0}


def test_emit_outputs_meta_fields(tmp_path: Path):
    player = PlayerOutput(
        mlbam_id=1,
        name="MetaTest",
        team="TST",
        age=25,
        role="H",
        position="SS",
        status_t=["contract"],
        tvp=1.75,
        tvp_p10=1.0,
        tvp_p50=2.0,
        tvp_p90=3.0,
        talent_value_p50=2.5,
        tvp_mean=2.0,
        tvp_std=0.5,
        tvp_risk_adj=1.75,
        flags={},
        contract_source=None,
        contract_confidence=None,
        late_negative_surplus_years=0,
        breakdown=[],
        service_time="01/2026",
        components=None,
    )
    meta_extra = {
        "pricing_regime": "nominal",
        "P0": 12.0,
        "g": 0.05,
        "discount_rate": 0.09,
        "risk_aversion_lambda": 0.5,
        "trade_currency": "tvp_mean_minus_lambda_std",
        "p0_calibration": {"samples": 10},
    }
    json_path, csv_path = emit_outputs(
        tmp_path,
        "2026-01-01",
        "bWAR",
        [player],
        1,
        prefix="top",
        rank_by="tvp_risk_adj",
        meta_extra=meta_extra,
    )
    payload = json.loads(Path(json_path).read_text())
    meta = payload["meta"]
    assert meta["rank_by"] == "tvp_risk_adj"
    for key in ("pricing_regime", "P0", "g", "discount_rate", "risk_aversion_lambda", "trade_currency", "p0_calibration"):
        assert key in meta
    first_line = Path(csv_path).read_text().splitlines()[0]
    assert first_line.startswith("# meta: ")


def test_emit_ranked_outputs_includes_ranks(tmp_path: Path):
    player = PlayerOutput(
        mlbam_id=1,
        name="RankTest",
        team="TST",
        age=25,
        role="H",
        position="SS",
        status_t=["contract"],
        tvp=1.75,
        tvp_p10=1.0,
        tvp_p50=2.0,
        tvp_p90=3.0,
        talent_value_p50=2.5,
        tvp_mean=2.0,
        tvp_std=0.5,
        tvp_risk_adj=1.75,
        flags={},
        contract_source=None,
        contract_confidence=None,
        late_negative_surplus_years=0,
        breakdown=[],
        service_time="01/2026",
        components=None,
    )
    ranks = {"tvp_risk_adj": {1: 5}, "talent_value_p50": {1: 2}}
    json_path, csv_path = emit_ranked_outputs(
        tmp_path,
        "2026-01-01",
        "bWAR",
        [player],
        ranks,
        1,
        prefix="top",
        label="combined_test",
    )
    payload = json.loads(Path(json_path).read_text())
    entry = payload["players"][0]
    assert entry["rank_trade_value"] == 5
    assert entry["rank_best_players"] == 2
    first_line = Path(csv_path).read_text().splitlines()[0]
    assert first_line.startswith("# meta: ")


def test_proration_applied_to_t0_breakdown():
    breakdown = build_breakdown(
        snapshot_year=2026,
        war_path=[2.0, 2.0],
        contract_years=[ContractYear(season=2026, cost_m=4.0, basis="guaranteed"), ContractYear(season=2027, cost_m=4.0, basis="guaranteed")],
        war_price_by_year=[10.0, 10.0],
        discount_rate=0.0,
        in_season_fraction=0.5,
    )
    assert breakdown[0]["war"] == pytest.approx(1.0)
    assert breakdown[0]["cost"] == pytest.approx(2.0)
    assert breakdown[1]["war"] == pytest.approx(2.0)


def test_discounting_year_indexing():
    breakdown = build_breakdown(
        snapshot_year=2026,
        war_path=[1.0, 1.0],
        contract_years=[ContractYear(season=2026, cost_m=0.0, basis="guaranteed"), ContractYear(season=2027, cost_m=0.0, basis="guaranteed")],
        war_price_by_year=[10.0, 10.0],
        discount_rate=0.1,
        in_season_fraction=1.0,
    )
    assert breakdown[0]["discount"] == pytest.approx(1.0)
    assert breakdown[1]["discount"] == pytest.approx(1.0 / 1.1)


def test_player_eligibility_filter():
    assert is_player_eligible(None, {}) is False
    record = ServiceTimeRecord(mlbam_id=1, service_time_years=1, service_time_days=0)
    assert is_player_eligible(record, {}) is True
    assert is_player_eligible(None, {2025: {"pa": 10.0}}) is True


def test_prior_degrades_for_no_usage():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    usage = {2025: {"pa": 0.0}}
    assert usage_prior_for_player("H", usage, config) == config.usage_prior.get("bench", 150.0)
    usage = {2025: {"ip": 0.0}}
    assert usage_prior_for_player("SP", usage, config) == config.usage_prior.get("rp", 60.0)


def test_seasons_with_usage_counts():
    history = [SeasonHistory(season=2025, war=1.0, usage=0.0), SeasonHistory(season=2024, war=1.0, usage=10.0)]
    assert seasons_with_usage(history) == 1


def test_risk_adjusted_value_penalizes_variance_without_changing_p50():
    samples_low = [0.0, 0.0, 0.0, 0.0, 0.0]
    samples_high = [-1.0, 0.0, 0.0, 0.0, 1.0]
    p50_low = compute_quantiles(samples_low, [0.5])["p50"]
    p50_high = compute_quantiles(samples_high, [0.5])["p50"]
    assert p50_low == p50_high == 0.0
    mean_low = sum(samples_low) / len(samples_low)
    mean_high = sum(samples_high) / len(samples_high)
    std_low = 0.0
    std_high = (sum((v - mean_high) ** 2 for v in samples_high) / len(samples_high)) ** 0.5
    risk_low = risk_adjusted_value(mean_low, std_low, 0.5)
    risk_high = risk_adjusted_value(mean_high, std_high, 0.5)
    assert risk_high < risk_low


def test_status_t_nonempty_for_contract_schedule():
    schedule = build_contract_schedule(
        contract={"contract_years": [{"season": 2026, "salary_m": 10.0}]},
        snapshot_year=2026,
        horizon_years=1,
        control_year_types=[],
        expected_war=[1.0],
        war_price_by_year=[10.0],
        arb_share=[0.5],
        min_salary_m=1.0,
        min_salary_growth=0.0,
    )
    timeline = ControlTimeline([])
    status = build_status_t(timeline, schedule)
    assert status == ["contract"]


def test_late_negative_surplus_years_counts_tail():
    breakdown = [
        {"surplus": 1.0},
        {"surplus": -1.0},
        {"surplus": -0.5},
    ]
    assert late_negative_surplus_years(breakdown, tail_years=2) == 2
    assert late_negative_surplus_years(breakdown, tail_years=3) == 2


def test_backloaded_contract_detection():
    schedule = build_contract_schedule(
        contract={
            "contract_years": [
                {"season": 2026, "salary_m": 10.0},
                {"season": 2027, "salary_m": 10.0},
                {"season": 2028, "salary_m": 10.0},
                {"season": 2029, "salary_m": 30.0},
                {"season": 2030, "salary_m": 30.0},
                {"season": 2031, "salary_m": 30.0},
            ]
        },
        snapshot_year=2026,
        horizon_years=6,
        control_year_types=[],
        expected_war=[1.0] * 6,
        war_price_by_year=[10.0] * 6,
        arb_share=[0.5],
        min_salary_m=1.0,
        min_salary_growth=0.0,
    )
    assert backloaded_contract(schedule, threshold=1.5) is True


def test_contract_override_uses_aav_and_basis():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    override = {
        "basis": "cbt_aav",
        "aav_m": 40.0,
        "term_start": 2025,
        "term_years": 3,
    }
    config = replace(config, contract_overrides={999: override})
    contract = {
        "contract_years": [{"season": 2026, "salary_m": 2.0}],
        "years_remaining": 5,
        "guaranteed_years_remaining": 5,
    }
    adjusted, basis = apply_contract_overrides(contract, 999, 2026, config)
    assert basis == "cbt_aav"
    assert adjusted["guaranteed_years_remaining"] == 2
    schedule = build_contract_schedule(
        adjusted,
        snapshot_year=2026,
        horizon_years=2,
        control_year_types=[],
        expected_war=[],
        war_price_by_year=[],
        arb_share=[0.5],
        min_salary_m=1.0,
        min_salary_growth=0.0,
        guaranteed_basis=basis,
    )
    assert [year.cost_m for year in schedule.years] == [40.0, 40.0]
    assert all(year.basis == "cbt_aav" for year in schedule.years)


def test_deferral_aav_policy_detects_large_gap():
    contract = {
        "aav_m": 10.0,
        "contract_years": [
            {"season": 2026, "salary_m": 3.0},
            {"season": 2027, "salary_m": 3.0},
        ],
    }
    assert should_use_aav_for_deferrals(contract, 2026, 1.3) is True


def test_hybrid_projection_role_defaults_to_config():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(config, hybrid_default_role="H")
    role = resolve_projection_role("HYB", {2025: {"pa": 10.0, "ip": 10.0}}, None, config)
    assert role == "H"


def test_role_classification_ignores_trivial_pitcher_pa():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    usage = {2025: {"ip": 55.0, "pa": 5.0, "g": 20.0, "gs": 20.0}}
    role, _ = determine_role(usage, config)
    assert role == "SP"


def test_role_classification_true_two_way():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    usage = {2025: {"ip": 50.0, "pa": 300.0, "g": 20.0, "gs": 20.0}}
    role, _ = determine_role(usage, config)
    assert role == "HYB"


def test_pitcher_durability_used_when_role_resolves_to_pitcher():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        durability_hit=replace(config.durability_hit, lost=0.01),
        durability_pitch=replace(config.durability_pitch, lost=0.2),
    )
    player = {
        "mlbam_id": 123,
        "name": "Test Pitcher",
        "team": "TST",
        "age": 26,
        "contract": {},
        "war": {"war_2023": 1.0, "war_2024": 1.0, "war_2025": 1.0},
        "usage": {2025: {"ip": 55.0, "pa": 5.0, "g": 20.0, "gs": 20.0}},
        "role": "HYB",
        "gs_share": 1.0,
        "service_time": ServiceTimeRecord(mlbam_id=123, service_time_years=3, service_time_days=0),
        "position": "P",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.flags["pitcher_tail_risk"] is True


def test_prospect_like_contract_guardrail_ignores_guaranteed():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(config, verified_extension_ids=set())
    player = {
        "mlbam_id": 999,
        "name": "Prospect",
        "team": "TST",
        "age": 20,
        "contract": {
            "contract_years": [
                {"season": 2026, "salary_m": 10.0, "is_guaranteed": True},
                {"season": 2027, "salary_m": 10.0, "is_guaranteed": True},
            ],
            "aav_m": 10.0,
        },
        "war": {"war_2023": 0.0, "war_2024": 0.0, "war_2025": 0.5},
        "usage": {2025: {"pa": 100.0}},
        "role": "H",
        "gs_share": None,
        "service_time": ServiceTimeRecord(mlbam_id=999, service_time_years=0, service_time_days=0),
        "position": "OF",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.flags["contract_ignored_prospect_like"] is True
    assert output.breakdown[0]["cost_basis"].startswith("model_cost")


def test_verified_extension_uses_contract():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(config, verified_extension_ids={999})
    player = {
        "mlbam_id": 999,
        "name": "Verified",
        "team": "TST",
        "age": 22,
        "contract": {
            "contract_years": [
                {"season": 2026, "salary_m": 10.0, "is_guaranteed": True},
                {"season": 2027, "salary_m": 10.0, "is_guaranteed": True},
            ],
            "aav_m": 10.0,
        },
        "war": {"war_2023": 0.0, "war_2024": 0.0, "war_2025": 1.5},
        "usage": {2025: {"pa": 250.0}},
        "role": "H",
        "gs_share": None,
        "service_time": ServiceTimeRecord(mlbam_id=999, service_time_years=0, service_time_days=200),
        "position": "OF",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.flags["contract_ignored_prospect_like"] is False
    assert output.breakdown[0]["cost_basis"] == "guaranteed"


def test_leaderboard_eligibility_thresholds():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    service = ServiceTimeRecord(mlbam_id=1, service_time_years=0, service_time_days=0)
    assert leaderboard_eligible("H", {2025: {"pa": 199.0}}, service, config) is False
    assert leaderboard_eligible("H", {2025: {"pa": 200.0}}, service, config) is True
    assert leaderboard_eligible("SP", {2025: {"ip": 49.9}}, service, config) is False
    assert leaderboard_eligible("SP", {2025: {"ip": 50.0}}, service, config) is True
    service_full = ServiceTimeRecord(mlbam_id=2, service_time_years=0, service_time_days=0)
    assert leaderboard_eligible("H", {}, service_full, replace(config, leaderboard_min_service_days=172)) is False
    service_full = ServiceTimeRecord(mlbam_id=2, service_time_years=0, service_time_days=172)
    assert leaderboard_eligible("H", {}, service_full, config) is True


def test_pitcher_std_exceeds_hitter_std_with_higher_shock_and_tails():
    durability_hit = DurabilityMixture([DurabilityState("full", 1.0, 1.0)])
    durability_pitch = DurabilityMixture(
        [DurabilityState("full", 0.7, 1.0), DurabilityState("lost", 0.3, 0.0)]
    )
    base_inputs = dict(
        rate_post=2.5,
        usage_post=600.0,
        age=25,
        denom=600.0,
        aging=AgingCurve(peak_age=27, rate_delta_before=0.0, rate_delta_after=0.0, usage_delta_before=0.0, usage_delta_after=0.0),
        horizon_years=2,
        war_price_by_year=[10.0, 10.0],
        discount_rate=0.0,
        contract_years=[ContractYear(season=2026, cost_m=0.0, basis="guaranteed"), ContractYear(season=2027, cost_m=0.0, basis="guaranteed")],
        in_season_fraction=1.0,
        role_prob_sp=None,
    )
    hit_cfg = SimulationConfig(sims=200, year_shock_sd=0.35, talent_sd=0.0)
    pitch_cfg = SimulationConfig(sims=200, year_shock_sd=0.5, talent_sd=0.0)
    hit_inputs = SimulationInputs(**base_inputs, durability=durability_hit)
    pitch_inputs = SimulationInputs(**base_inputs, durability=durability_pitch)
    hit_result = simulate_tvp(hit_cfg, hit_inputs, expected_war=[2.5, 2.5])
    pitch_result = simulate_tvp(pitch_cfg, pitch_inputs, expected_war=[2.5, 2.5])
    assert pitch_result.std > hit_result.std


def test_pitcher_tail_increase_lowers_risk_adj():
    durability_base = DurabilityMixture(
        [DurabilityState("full", 0.8, 1.0), DurabilityState("lost", 0.2, 0.0)]
    )
    durability_tail = DurabilityMixture(
        [DurabilityState("full", 0.6, 1.0), DurabilityState("lost", 0.4, 0.0)]
    )
    sim_cfg = SimulationConfig(sims=200, year_shock_sd=0.5, talent_sd=0.0)
    inputs = SimulationInputs(
        rate_post=2.0,
        usage_post=180.0,
        age=25,
        denom=180.0,
        aging=AgingCurve(peak_age=27, rate_delta_before=0.0, rate_delta_after=0.0, usage_delta_before=0.0, usage_delta_after=0.0),
        horizon_years=2,
        war_price_by_year=[10.0, 10.0],
        discount_rate=0.0,
        contract_years=[ContractYear(season=2026, cost_m=0.0, basis="guaranteed"), ContractYear(season=2027, cost_m=0.0, basis="guaranteed")],
        durability=durability_base,
        in_season_fraction=1.0,
        role_prob_sp=None,
    )
    base_result = simulate_tvp(sim_cfg, inputs, expected_war=[2.0, 2.0])
    tail_result = simulate_tvp(sim_cfg, replace(inputs, durability=durability_tail), expected_war=[2.0, 2.0])
    base_adj = risk_adjusted_value(base_result.mean, base_result.std, 0.6)
    tail_adj = risk_adjusted_value(tail_result.mean, tail_result.std, 0.6)
    assert tail_adj < base_adj


def test_metric_missing_ops_plus_matches_baseline():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        metric_enabled=True,
        ops_plus_coef=0.1,
        metric_cap_hitter=0.25,
        min_ops_pa_total=400.0,
    )
    player = {
        "mlbam_id": 9001,
        "name": "NoOPS",
        "team": "TST",
        "age": 25,
        "contract": {},
        "war": {"war_2023": 6.0, "war_2024": 6.0, "war_2025": 6.0},
        "usage": {2023: {"pa": 600.0}, 2024: {"pa": 600.0}, 2025: {"pa": 600.0}},
        "role": "H",
        "gs_share": None,
        "service_time": ServiceTimeRecord(mlbam_id=9001, service_time_years=1, service_time_days=0),
        "position": "SS",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.metric_adjustment_raw == pytest.approx(0.0)
    assert output.metric_adjustment_clamped == pytest.approx(0.0)
    assert output.war_rate_post_final == pytest.approx(output.war_rate_war)


def test_metric_missing_fip_matches_baseline():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        metric_enabled=True,
        fip_coef=1.0,
        metric_cap_pitcher=0.25,
        min_fip_ip_total=80.0,
    )
    player = {
        "mlbam_id": 9002,
        "name": "NoFIP",
        "team": "TST",
        "age": 26,
        "contract": {},
        "war": {"war_2023": 4.0, "war_2024": 4.0, "war_2025": 4.0},
        "usage": {2023: {"ip": 180.0, "g": 30.0, "gs": 30.0}, 2024: {"ip": 180.0, "g": 30.0, "gs": 30.0}, 2025: {"ip": 180.0, "g": 30.0, "gs": 30.0}},
        "role": "SP",
        "gs_share": 1.0,
        "service_time": ServiceTimeRecord(mlbam_id=9002, service_time_years=1, service_time_days=0),
        "position": "SP",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.metric_adjustment_raw == pytest.approx(0.0)
    assert output.metric_adjustment_clamped == pytest.approx(0.0)
    assert output.war_rate_post_final == pytest.approx(output.war_rate_war)


def test_metric_cap_enforced_for_hitter():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        metric_enabled=True,
        ops_plus_coef=0.1,
        metric_cap_hitter=0.25,
        min_ops_pa_total=400.0,
    )
    usage = {
        2023: {"pa": 600.0, "ops_plus": 200.0},
        2024: {"pa": 600.0, "ops_plus": 200.0},
        2025: {"pa": 600.0, "ops_plus": 200.0},
    }
    player = {
        "mlbam_id": 9003,
        "name": "CapHit",
        "team": "TST",
        "age": 25,
        "contract": {},
        "war": {"war_2023": 6.0, "war_2024": 6.0, "war_2025": 6.0},
        "usage": usage,
        "role": "H",
        "gs_share": None,
        "service_time": ServiceTimeRecord(mlbam_id=9003, service_time_years=1, service_time_days=0),
        "position": "SS",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    cap = abs(output.war_rate_war) * 0.25
    assert output.metric_adjustment_clamped == pytest.approx(cap)
    assert abs(output.metric_adjustment_clamped) <= abs(output.war_rate_war) * 0.30


def test_metric_cap_enforced_for_pitcher():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        metric_enabled=True,
        fip_coef=1.0,
        metric_cap_pitcher=0.25,
        min_fip_ip_total=80.0,
    )
    usage = {
        2023: {"ip": 180.0, "g": 30.0, "gs": 30.0, "fip": 2.0, "lg_fip": 4.5},
        2024: {"ip": 180.0, "g": 30.0, "gs": 30.0, "fip": 2.0, "lg_fip": 4.5},
        2025: {"ip": 180.0, "g": 30.0, "gs": 30.0, "fip": 2.0, "lg_fip": 4.5},
    }
    player = {
        "mlbam_id": 9004,
        "name": "CapPit",
        "team": "TST",
        "age": 26,
        "contract": {},
        "war": {"war_2023": 4.0, "war_2024": 4.0, "war_2025": 4.0},
        "usage": usage,
        "role": "SP",
        "gs_share": 1.0,
        "service_time": ServiceTimeRecord(mlbam_id=9004, service_time_years=1, service_time_days=0),
        "position": "SP",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    cap = abs(output.war_rate_war) * 0.25
    assert output.metric_adjustment_clamped == pytest.approx(cap)
    assert abs(output.metric_adjustment_clamped) <= abs(output.war_rate_war) * 0.30


def test_metrics_use_last_three_seasons_only():
    config = load_config(Path("backend/tvp_config.json"), "bWAR")
    config = replace(
        config,
        metric_enabled=True,
        ops_plus_coef=0.1,
        metric_cap_hitter=0.25,
        min_ops_pa_total=400.0,
    )
    usage = {
        2022: {"pa": 600.0, "ops_plus": 200.0},
        2023: {"pa": 600.0, "ops_plus": 100.0},
        2024: {"pa": 600.0, "ops_plus": 100.0},
        2025: {"pa": 600.0, "ops_plus": 100.0},
    }
    player = {
        "mlbam_id": 9005,
        "name": "WindowOnly",
        "team": "TST",
        "age": 25,
        "contract": {},
        "war": {"war_2023": 6.0, "war_2024": 6.0, "war_2025": 6.0},
        "usage": usage,
        "role": "H",
        "gs_share": None,
        "service_time": ServiceTimeRecord(mlbam_id=9005, service_time_years=1, service_time_days=0),
        "position": "SS",
    }
    output = build_player_output(player, config, 2026, 1.0, set())
    assert output is not None
    assert output.ops_plus_3yr == pytest.approx(100.0)
