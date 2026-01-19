import math
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


if __name__ == "__main__":
    unittest.main()
