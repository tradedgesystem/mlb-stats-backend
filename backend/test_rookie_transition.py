import math
import sys
import unittest
from pathlib import Path

from compute_mlb_tvp import (
    adjust_player_age,
    compute_player_tvp,
    compute_weighted_fwar,
    enrich_players,
    load_pitcher_names,
    load_players,
    load_prospect_anchors,
    load_reliever_names,
    load_sample_counts,
    load_two_way_names,
    load_war_history,
)
from tvp_engine import compute_prospect_tvp, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
from audit_rookie_transition import collect_audit_rows  # noqa: E402
from explain_top25_rookie_transition import build_explain_row  # noqa: E402


class TestRookieTransition(unittest.TestCase):
    def _compute(self, player: dict, war_history: dict, fwar_weights: list[float]):
        repo_root = Path(__file__).resolve().parent
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        fwar_weight_seasons = [
            snapshot_year - offset for offset in range(len(fwar_weights))
        ]
        return compute_player_tvp(
            player,
            snapshot_year,
            config_path,
            max_years=10,
            fwar_scale=0.7,
            fwar_cap=6.0,
            apply_aging=True,
            prime_age=29,
            decline_per_year=0.035,
            aging_floor=0.65,
            reliever_names=set(),
            reliever_mult=1.5,
            control_years_fallback=0,
            control_years_age_max=27,
            two_way_names=set(),
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history=war_history,
            fwar_weights=fwar_weights,
            fwar_weight_seasons=fwar_weight_seasons,
            pitcher_names=set(),
            pitcher_regress_weight=0.0,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )

    def _compute_with_pitcher_flag(
        self,
        player: dict,
        war_history: dict,
        fwar_weights: list[float],
    ):
        repo_root = Path(__file__).resolve().parent
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        fwar_weight_seasons = [
            snapshot_year - offset for offset in range(len(fwar_weights))
        ]
        name_key = player.get("player_name", "").lower().replace(" ", "")
        return compute_player_tvp(
            player,
            snapshot_year,
            config_path,
            max_years=10,
            fwar_scale=0.7,
            fwar_cap=6.0,
            apply_aging=True,
            prime_age=29,
            decline_per_year=0.035,
            aging_floor=0.65,
            reliever_names=set(),
            reliever_mult=1.5,
            control_years_fallback=0,
            control_years_age_max=27,
            two_way_names=set(),
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history=war_history,
            fwar_weights=fwar_weights,
            fwar_weight_seasons=fwar_weight_seasons,
            pitcher_names={name_key},
            pitcher_regress_weight=0.0,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )

    def test_rookie_blend_applies_low_pa(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "Rookie Blend Test",
            "age": 20,
            "fv_value": 60,
            "eta": snapshot_year,
            "position": "CF",
            "top_100_rank": 10,
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 2001,
            "player_name": "Rookie Blend Test",
            "age": 22,
            "fwar": 2.0,
            "pa": 120,
            "bat_war": 2.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history={}, fwar_weights=[1.0])
        transition = result["raw_components"]["rookie_transition"]
        self.assertTrue(transition["applied"])
        self.assertLess(transition["alpha"], 1.0)
        self.assertFalse(math.isclose(result["tvp_current"], result["tvp_mlb"]))
        self.assertIn("tvp_current_pre", transition)
        self.assertIn("tvp_current_post", transition)
        self.assertIn("delta", transition)
        self.assertFalse(math.isclose(transition["delta"], 0.0))
        self.assertTrue(
            math.isclose(
                transition["delta"],
                transition["tvp_current_post"] - transition["tvp_current_pre"],
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        )
        tvp_prospect = transition["prospect_tvp"]
        tvp_current = result["tvp_current"]
        self.assertIsNotNone(tvp_prospect)
        self.assertGreaterEqual(tvp_current, min(result["tvp_mlb"], tvp_prospect))
        self.assertLessEqual(tvp_current, max(result["tvp_mlb"], tvp_prospect))

    def test_rookie_blend_skips_high_pa(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "Veteran Blend Test",
            "age": 22,
            "fv_value": 55,
            "eta": snapshot_year,
            "position": "SS",
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 2002,
            "player_name": "Veteran Blend Test",
            "age": 27,
            "fwar": 3.0,
            "pa": 2500,
            "bat_war": 3.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history={}, fwar_weights=[1.0])
        transition = result["raw_components"]["rookie_transition"]
        self.assertFalse(transition["applied"])
        self.assertTrue(math.isclose(result["tvp_current"], result["tvp_mlb"]))

    def test_fallback_anchor_noop(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player = {
            "mlb_id": 2003,
            "player_name": "Fallback Anchor Test",
            "age": 24,
            "fwar": 1.0,
            "pa": 200.0,
            "bat_war": 1.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history={}, fwar_weights=[1.0])
        transition = result["raw_components"]["rookie_transition"]
        self.assertFalse(transition["applied"])
        self.assertEqual(transition.get("reason_not_applied"), "fallback_anchor_noop")
        self.assertEqual(transition.get("anchor_source"), "mlb_baseline_fallback")
        self.assertTrue(math.isclose(transition.get("delta", 0.0), 0.0))

    def test_integration_rookie_transition_pca(self) -> None:
        repo_root = Path(__file__).resolve().parent
        players_path = repo_root / "output" / "players_with_contracts_2025.json"
        stats_db_path = repo_root / "stats.db"
        if not players_path.exists() or not stats_db_path.exists():
            self.skipTest("Missing local players/DB fixtures.")

        payload = load_players(players_path)
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        snapshot_season = payload.get("meta", {}).get("season", snapshot_year)
        age_offset = max(0, snapshot_year - snapshot_season)

        reliever_names = load_reliever_names(snapshot_season, stats_db_path)
        pitcher_names = load_pitcher_names(snapshot_season, stats_db_path)
        two_way_names = load_two_way_names(snapshot_season, stats_db_path, 1.0)
        fwar_weights = [0.5, 0.3, 0.2]
        fwar_weight_seasons = [
            snapshot_season - offset for offset in range(len(fwar_weights))
        ]
        war_history = load_war_history(fwar_weight_seasons, stats_db_path)
        sample_counts = load_sample_counts(snapshot_season, stats_db_path)
        prospects_by_id, prospects_by_name = load_prospect_anchors(repo_root.parents[0])
        enrich_players(payload.get("players", []), sample_counts, prospects_by_id, prospects_by_name)

        player = next(
            (p for p in payload.get("players", []) if p.get("mlb_id") == 691718),
            None,
        )
        if not player:
            self.skipTest("PCA not found in players file.")
        result = compute_player_tvp(
            adjust_player_age(player, age_offset),
            snapshot_year,
            config_path,
            max_years=10,
            fwar_scale=0.7,
            fwar_cap=6.0,
            apply_aging=True,
            prime_age=29,
            decline_per_year=0.035,
            aging_floor=0.65,
            reliever_names=reliever_names,
            reliever_mult=1.5,
            control_years_fallback=4,
            control_years_age_max=27,
            two_way_names=two_way_names,
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history=war_history,
            fwar_weights=fwar_weights,
            fwar_weight_seasons=fwar_weight_seasons,
            pitcher_names=pitcher_names,
            pitcher_regress_weight=0.35,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )
        transition = result["raw_components"].get("rookie_transition", {})
        if player.get("prospect_anchor") is None:
            self.assertFalse(transition.get("applied", False))
        else:
            self.assertIn("alpha", transition)

    def test_integration_anchor_link_by_mlb_id(self) -> None:
        repo_root = Path(__file__).resolve().parent
        players_path = repo_root / "output" / "players_with_contracts_2025.json"
        prospects_path = repo_root / "output" / "tvp_prospects_2026_final.json"
        stats_db_path = repo_root / "stats.db"
        if (
            not players_path.exists()
            or not stats_db_path.exists()
            or not prospects_path.exists()
        ):
            self.skipTest("Missing local players/DB/prospects fixtures.")

        payload = load_players(players_path)
        prospects = load_players(prospects_path).get("prospects", [])
        prospect_ids = {p.get("mlb_id") for p in prospects if isinstance(p.get("mlb_id"), int)}
        players = payload.get("players", [])
        candidate = next(
            (p for p in players if p.get("mlb_id") in prospect_ids),
            None,
        )
        if not candidate:
            self.skipTest("No overlapping mlb_id between players and prospects.")

        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        snapshot_season = payload.get("meta", {}).get("season", snapshot_year)
        sample_counts = load_sample_counts(snapshot_season, stats_db_path)
        prospects_by_id, prospects_by_name = load_prospect_anchors(repo_root.parents[0])
        enrich_players(players, sample_counts, prospects_by_id, prospects_by_name)
        self.assertIsNotNone(candidate.get("prospect_anchor"))

    def test_integration_rookie_transition_cal_raleigh(self) -> None:
        repo_root = Path(__file__).resolve().parent
        players_path = repo_root / "output" / "players_with_contracts_2025.json"
        stats_db_path = repo_root / "stats.db"
        if not players_path.exists() or not stats_db_path.exists():
            self.skipTest("Missing local players/DB fixtures.")

        payload = load_players(players_path)
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        snapshot_season = payload.get("meta", {}).get("season", snapshot_year)
        age_offset = max(0, snapshot_year - snapshot_season)

        reliever_names = load_reliever_names(snapshot_season, stats_db_path)
        pitcher_names = load_pitcher_names(snapshot_season, stats_db_path)
        two_way_names = load_two_way_names(snapshot_season, stats_db_path, 1.0)
        fwar_weights = [0.5, 0.3, 0.2]
        fwar_weight_seasons = [
            snapshot_season - offset for offset in range(len(fwar_weights))
        ]
        war_history = load_war_history(fwar_weight_seasons, stats_db_path)
        sample_counts = load_sample_counts(snapshot_season, stats_db_path)
        prospects_by_id, prospects_by_name = load_prospect_anchors(repo_root.parents[0])
        enrich_players(payload.get("players", []), sample_counts, prospects_by_id, prospects_by_name)

        player = next(
            (p for p in payload.get("players", []) if p.get("mlb_id") == 663728),
            None,
        )
        if not player:
            self.skipTest("Cal Raleigh not found in players file.")
        result = compute_player_tvp(
            adjust_player_age(player, age_offset),
            snapshot_year,
            config_path,
            max_years=10,
            fwar_scale=0.7,
            fwar_cap=6.0,
            apply_aging=True,
            prime_age=29,
            decline_per_year=0.035,
            aging_floor=0.65,
            reliever_names=reliever_names,
            reliever_mult=1.5,
            control_years_fallback=4,
            control_years_age_max=27,
            two_way_names=two_way_names,
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history=war_history,
            fwar_weights=fwar_weights,
            fwar_weight_seasons=fwar_weight_seasons,
            pitcher_names=pitcher_names,
            pitcher_regress_weight=0.35,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )
        transition = result["raw_components"].get("rookie_transition", {})
        self.assertFalse(transition.get("applied", False))
        self.assertTrue(math.isclose(result["tvp_current"], result["tvp_mlb"]))

    def test_war_history_prefers_mlb_id(self) -> None:
        war_history = {
            123: {2025: {"bat": 2.0, "pit": 0.0}},
            "namekey": {2025: {"bat": 5.0, "pit": 0.0}},
        }
        weighted, meta = compute_weighted_fwar(
            "namekey", [2025], [1.0], war_history, mlb_id=123
        )
        self.assertTrue(math.isclose(weighted or 0.0, 2.0))
        self.assertEqual(meta.get("history_key"), 123)

    def test_should_have_applied_excludes_non_early_sample(self) -> None:
        players = [
            {
                "player_name": "Early Sample Missing Anchor",
                "mlb_id": 3001,
                "age": 23,
                "tvp_mlb": 10.0,
                "tvp_current": 10.0,
                "raw_components": {
                    "war_inputs": {"is_pitcher": False, "war_history_seasons_used": 1},
                    "rookie_transition": {
                        "applied": False,
                        "reason_not_applied": "missing_anchor",
                        "pa": 150.0,
                        "ip": None,
                        "tvp_current_pre": 10.0,
                        "tvp_current_post": 10.0,
                        "delta": 0.0,
                    },
                },
            },
            {
                "player_name": "Veteran Missing Anchor",
                "mlb_id": 3002,
                "age": 29,
                "tvp_mlb": 20.0,
                "tvp_current": 20.0,
                "raw_components": {
                    "war_inputs": {"is_pitcher": False, "war_history_seasons_used": 5},
                    "rookie_transition": {
                        "applied": False,
                        "reason_not_applied": "missing_anchor",
                        "pa": 400.0,
                        "ip": None,
                        "tvp_current_pre": 20.0,
                        "tvp_current_post": 20.0,
                        "delta": 0.0,
                    },
                },
            },
            {
                "player_name": "Pitcher Missing Anchor",
                "mlb_id": 3003,
                "age": 28,
                "tvp_mlb": 30.0,
                "tvp_current": 30.0,
                "raw_components": {
                    "war_inputs": {"is_pitcher": True, "war_history_seasons_used": 2},
                    "rookie_transition": {
                        "applied": False,
                        "reason_not_applied": "missing_anchor",
                        "pa": 0.0,
                        "ip": 120.0,
                        "tvp_current_pre": 30.0,
                        "tvp_current_post": 30.0,
                        "delta": 0.0,
                    },
                },
            },
        ]
        applied, missing_candidates, _ = collect_audit_rows(players)
        self.assertEqual(len(applied), 0)
        self.assertEqual(len(missing_candidates), 1)
        self.assertEqual(missing_candidates[0]["mlb_id"], 3001)

    def test_pa_threshold_excludes_early_sample(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "High PA Gate Test",
            "age": 21,
            "fv_value": 55,
            "eta": snapshot_year,
            "position": "CF",
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 4001,
            "player_name": "High PA Gate Test",
            "age": 23,
            "fwar": 2.0,
            "pa": 400,
            "bat_war": 2.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history={}, fwar_weights=[1.0])
        transition = result["raw_components"]["rookie_transition"]
        self.assertFalse(transition["applied"])
        self.assertFalse(transition.get("early_sample_eligible"))

    def test_ip_threshold_excludes_early_sample(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "High IP Gate Test",
            "age": 22,
            "fv_value": 55,
            "eta": snapshot_year,
            "position": "RHP",
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 4002,
            "player_name": "High IP Gate Test",
            "age": 24,
            "fwar": 2.0,
            "ip": 120.0,
            "pit_war": 2.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute_with_pitcher_flag(
            player, war_history={}, fwar_weights=[1.0]
        )
        transition = result["raw_components"]["rookie_transition"]
        self.assertFalse(transition["applied"])
        self.assertFalse(transition.get("early_sample_eligible"))

    def test_age_cap_excludes_relief_low_ip(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "Age Cap Reliever",
            "age": 22,
            "fv_value": 55,
            "eta": snapshot_year,
            "position": "RHP",
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 5001,
            "player_name": "Age Cap Reliever",
            "age": 31,
            "fwar": 1.0,
            "ip": 60.0,
            "pit_war": 1.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute_with_pitcher_flag(
            player, war_history={}, fwar_weights=[1.0]
        )
        transition = result["raw_components"]["rookie_transition"]
        self.assertFalse(transition["applied"])
        self.assertFalse(transition.get("early_sample_eligible"))

    def test_age_cap_keeps_young_hitter_eligible(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        prospect = {
            "player_name": "Age Cap Hitter",
            "age": 20,
            "fv_value": 55,
            "eta": snapshot_year,
            "position": "SS",
        }
        anchor = compute_prospect_tvp(prospect, config)
        player = {
            "mlb_id": 5002,
            "player_name": "Age Cap Hitter",
            "age": 24,
            "fwar": 1.0,
            "pa": 200.0,
            "bat_war": 1.0,
            "prospect_anchor": {
                "tvp_prospect": anchor["tvp_prospect"],
                "fv_value": anchor["raw_components"]["fv_value"],
                "raw_components": anchor["raw_components"],
                "source_file": "unit_test",
            },
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 1.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history={}, fwar_weights=[1.0])
        transition = result["raw_components"]["rookie_transition"]
        self.assertTrue(transition.get("early_sample_eligible"))

    def test_explain_reason_codes_pa_threshold(self) -> None:
        player = {
            "player_name": "Explain Test Hitter",
            "mlb_id": 9001,
            "age": 24,
            "tvp_mlb": 10.0,
            "tvp_current": 10.0,
            "raw_components": {
                "rookie_transition": {"pa": 350.0, "ip": None},
                "war_inputs": {"war_history_seasons_used": 1},
            },
        }
        row = build_explain_row(player)
        self.assertFalse(row["early_sample_eligible"])
        self.assertEqual(row["reason_not_eligible"], "pa_ge_300")
        self.assertIn(
            row["reason_not_eligible"],
            {
                "pa_ge_300",
                "ip_ge_80",
                "missing_pa_ip_and_fallback_failed",
                "other",
            },
        )


if __name__ == "__main__":
    unittest.main()
