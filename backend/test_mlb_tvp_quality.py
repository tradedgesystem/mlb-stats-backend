import math
import unittest
from pathlib import Path
import json

from compute_mlb_tvp import compute_player_tvp, normalize_name
import compute_mlb_tvp
from tvp_engine import load_config


class TestMlbTvpQuality(unittest.TestCase):
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

    def test_weighted_history_partial_renormalized(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player_name = "Weighted History Partial"
        name_key = normalize_name(player_name)
        war_history = {name_key: {snapshot_year: {"bat": 1.0, "pit": 0.0}}}
        player = {
            "mlb_id": 100,
            "player_name": player_name,
            "age": 25,
            "fwar": 1.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history, [0.5, 0.3, 0.2])
        projection = result["raw_components"]["projection"]
        weighted_meta = projection["weighted_fwar_meta"]
        self.assertEqual(projection["fwar_source_label"], "history_weighted_partial")
        self.assertEqual(weighted_meta["seasons_count"], 1)
        self.assertTrue(math.isclose(weighted_meta["weights_sum"], 1.0))
        weights_total = sum(season["weight"] for season in weighted_meta["seasons"])
        self.assertTrue(math.isclose(weights_total, 1.0))

    def test_option_buyout_not_in_guaranteed_stream(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        option_year = snapshot_year + 2
        player_name = "Option Buyout Test"
        name_key = normalize_name(player_name)
        war_history = {name_key: {snapshot_year: {"bat": 2.0, "pit": 0.0}}}
        player = {
            "mlb_id": 101,
            "player_name": player_name,
            "age": 27,
            "fwar": 2.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True},
                    {
                        "season": snapshot_year + 1,
                        "salary_m": 6.0,
                        "is_guaranteed": True,
                    },
                    {"season": option_year, "salary_m": 10.0, "is_guaranteed": False},
                ],
                "options": [
                    {
                        "season": option_year,
                        "type": "CO",
                        "salary_m": 10.0,
                        "buyout_m": 2.0,
                    }
                ],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history, [1.0])
        projection = result["raw_components"]["projection"]
        contract = result["raw_components"]["contract"]
        option_seasons = contract["option_seasons"]
        self.assertEqual(option_seasons, [option_year])

        option_t = option_year - snapshot_year
        guaranteed_by_t = projection["guaranteed_fwar_by_t"]
        option_by_t = projection["option_fwar_by_t"]
        self.assertNotIn(str(option_t), guaranteed_by_t)
        self.assertIn(str(option_t), option_by_t)
        self.assertGreater(option_by_t[str(option_t)], 0.0)

        salary_by_year = result["raw_components"]["mlb"]["salary_by_year"]
        self.assertTrue(math.isclose(salary_by_year[option_t], 0.0))

        options = result["raw_components"]["options"]
        self.assertEqual(len(options), 1)
        option = options[0]
        self.assertEqual(option["t"], option_t)
        self.assertEqual(option["type"], "CO")
        self.assertTrue(math.isclose(option["B"], 2.0))
        self.assertTrue(math.isclose(option["S"], 10.0))
        self.assertIn("fwar_used_for_option", option)
        self.assertIsNotNone(option["fwar_used_for_option"])

    def test_no_option_seasons_no_trailing_year(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player_name = "No Option Test"
        name_key = normalize_name(player_name)
        war_history = {name_key: {snapshot_year: {"bat": 1.5, "pit": 0.0}}}
        player = {
            "mlb_id": 102,
            "player_name": player_name,
            "age": 26,
            "fwar": 1.5,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 4.0, "is_guaranteed": True},
                    {
                        "season": snapshot_year + 1,
                        "salary_m": 5.0,
                        "is_guaranteed": True,
                    },
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }
        result = self._compute(player, war_history, [1.0])
        projection = result["raw_components"]["projection"]
        contract = result["raw_components"]["contract"]
        self.assertEqual(contract["option_seasons"], [])
        self.assertEqual(projection["option_fwar_by_t"], {})
        guaranteed_by_t = projection["guaranteed_fwar_by_t"]
        self.assertEqual(len(guaranteed_by_t), 2)
        mlb_raw = result["raw_components"]["mlb"]
        self.assertEqual(len(mlb_raw["salary_by_year"]), 2)
        mlb_raw = result["raw_components"]["mlb"]
        self.assertEqual(len(mlb_raw["salary_by_year"]), 2)


class TestCatcherWarHaircut(unittest.TestCase):
    def _compute(
        self, player: dict, position: str, war_history: dict, fwar_weights: list[float]
    ):
        repo_root = Path(__file__).resolve().parent
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        fwar_weight_seasons = [
            snapshot_year - offset for offset in range(len(fwar_weights))
        ]

        player = dict(player)
        player["position"] = position
        player["position_source"] = "test_fixture"
        catcher_ids = (
            {player["mlb_id"]}
            if compute_mlb_tvp.is_catcher_position(position)
            else set()
        )

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
            catcher_ids=catcher_ids,
        )

    def test_catcher_war_haircut_applied(self) -> None:
        """Test that catcher TVP is reduced by catcher_war_mult"""
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player_name = "Test Catcher"
        name_key = normalize_name(player_name)

        # Create catcher player with same stats as non-catcher
        catcher_player = {
            "mlb_id": 900,
            "player_name": player_name,
            "age": 28,
            "fwar": 5.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True}
                ]
                * 5,
                "options": [],
                "aav_m": 5.0,
                "years_remaining": 5,
            },
        }

        non_catcher_player = {
            "mlb_id": 901,
            "player_name": "Test Non-Catcher",
            "age": 28,
            "fwar": 5.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True}
                ]
                * 5,
                "options": [],
                "aav_m": 5.0,
                "years_remaining": 5,
            },
        }

        war_history = {
            name_key: {snapshot_year: {"bat": 5.0, "pit": 0.0}},
            normalize_name("Test Non-Catcher"): {
                snapshot_year: {"bat": 5.0, "pit": 0.0}
            },
        }

        catcher_result = self._compute(
            catcher_player, "C", war_history, [0.5, 0.3, 0.2]
        )
        non_catcher_result = self._compute(
            non_catcher_player, "1B", war_history, [0.5, 0.3, 0.2]
        )

        # Verify catcher TVP is lower than non-catcher TVP
        catcher_tvp = catcher_result.get("tvp_mlb")
        non_catcher_tvp = non_catcher_result.get("tvp_mlb")

        self.assertIsNotNone(catcher_tvp)
        self.assertIsNotNone(non_catcher_tvp)

        # With catcher risk adjustments, catcher TVP should be lower than non-catcher TVP
        # Allow some tolerance for PV differences due to aging curves
        expected_ratio = 0.90
        actual_ratio = (
            catcher_tvp / non_catcher_tvp
            if non_catcher_tvp is not None and non_catcher_tvp > 0
            else 0
        )

        self.assertLess(
            actual_ratio, 1.0, "Catcher TVP should be less than non-catcher TVP"
        )
        self.assertGreater(
            actual_ratio,
            expected_ratio * 0.80,
            "Catcher TVP reduction should be in reasonable range",
        )

        # Verify audit trail
        projection = catcher_result["raw_components"]["projection"]
        self.assertTrue(projection.get("is_catcher"), "Catcher flag should be true")
        self.assertTrue(
            projection.get("catcher_risk_applied"),
            "Catcher risk adjustments should be applied",
        )
        self.assertIsNotNone(
            projection.get("fwar_before_catcher_risk"),
            "Should have pre-catcher-risk fWAR",
        )

    def test_non_catcher_unchanged(self) -> None:
        """Test that non-catcher TVP is unchanged when multiplier exists"""
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player_name = "Test Non-Catcher 2"

        player = {
            "mlb_id": 902,
            "player_name": player_name,
            "age": 28,
            "fwar": 5.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 5.0, "is_guaranteed": True}
                ]
                * 5,
                "options": [],
                "aav_m": 5.0,
                "years_remaining": 5,
            },
        }

        name_key = normalize_name(player_name)
        war_history = {
            name_key: {snapshot_year: {"bat": 5.0, "pit": 0.0}},
        }

        result = self._compute(player, "SS", war_history, [0.5, 0.3, 0.2])

        # Verify non-catcher TVP is computed correctly
        tvp_mlb = result.get("tvp_mlb")
        self.assertIsNotNone(tvp_mlb)

        # Verify audit trail shows catcher is false
        projection = result["raw_components"]["projection"]
        self.assertFalse(
            projection.get("is_catcher"), "Non-catcher flag should be false"
        )
        self.assertFalse(
            projection.get("catcher_risk_applied", False),
            "Catcher risk adjustments should not be applied",
        )

        # Verify no pre-catcher-risk fWAR (adjustments not applied)
        self.assertIsNone(
            projection.get("fwar_before_catcher_risk"),
            "Should not have pre-catcher-risk fWAR when not a catcher",
        )


if __name__ == "__main__":
    unittest.main()
