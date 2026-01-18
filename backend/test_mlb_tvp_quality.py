import json
import math
import unittest
from pathlib import Path
from typing import Optional

from compute_mlb_tvp import compute_player_tvp, normalize_name, load_players
from tvp_engine import load_config


class TestMlbTvpQuality(unittest.TestCase):
    def _compute(
        self,
        player: dict,
        war_history: dict,
        fwar_weights: list[float],
        pitcher_names: Optional[set[str]] = None,
    ):
        repo_root = Path(__file__).resolve().parent
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        fwar_weight_seasons = [
            snapshot_year - offset for offset in range(len(fwar_weights))
        ]
        result = compute_player_tvp(
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
            control_years_fallback=6,
            control_years_age_max=27,
            two_way_names=set(),
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history=war_history,
            fwar_weights=fwar_weights,
            fwar_weight_seasons=fwar_weight_seasons,
            pitcher_names=pitcher_names if pitcher_names is not None else set(),
            pitcher_regress_weight=0.0,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )
        return result

    def test_weighted_history_partial_renormalized(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player_name = "Weighted History Partial"
        name_key = normalize_name(player_name)
        war_history = {
            name_key: {
                snapshot_year: {"bat": 1.0, "pit": 0.0},
            }
        }
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
        war_history = {
            name_key: {
                snapshot_year: {"bat": 2.0, "pit": 0.0},
            }
        }
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
        guaranteed_by_t = projection["guaranteed_projection_by_t"]
        option_by_t = projection["option_year_projection_by_t"]
        self.assertTrue(math.isclose(guaranteed_by_t[str(option_t)], 0.0))
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
        self.assertIn("fwar", option)
        self.assertIsNotNone(option["fwar"])

    def test_arbitration_salary_model(self) -> None:
        """Verify arb salary matches max(min, ARB_SHARE * value)."""
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year

        player_name = "Arb Salary Test"
        name_key = normalize_name(player_name)
        war_history = {
            name_key: {
                snapshot_year: {"bat": 2.0, "pit": 0.0},
            }
        }

        player = {
            "mlb_id": 999,
            "player_name": player_name,
            "age": 25,
            "fwar": 2.0,
            "contract": {
                "contract_years": [],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }

        result = self._compute(player, war_history, [1.0])

        rc = result["raw_components"]
        control_fallback = rc["contract"].get("control_fallback", {})

        self.assertIn("salary_components_by_t", control_fallback)
        salary_components = control_fallback["salary_components_by_t"]

        self.assertIn("0", salary_components)

        year_0 = salary_components["0"]
        self.assertEqual(year_0["min_salary_t"], year_0["final_salary_t"])

        year_3 = salary_components["3"]
        self.assertGreater(year_3["arb_salary_t"], year_3["min_salary_t"])
        self.assertEqual(year_3["arb_share"], 0.3)

        year_4 = salary_components["4"]
        self.assertGreater(year_4["arb_salary_t"], year_4["min_salary_t"])
        self.assertEqual(year_4["arb_share"], 0.5)

        year_5 = salary_components["5"]
        self.assertGreater(year_5["arb_salary_t"], year_5["min_salary_t"])
        self.assertEqual(year_5["arb_share"], 0.7)

    def test_mean_reversion_applied(self) -> None:
        """Verify mean reversion is applied to young hitter."""
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year

        player_name = "Mean Revert Test"
        name_key = normalize_name(player_name)

        war_history = {name_key: {snapshot_year: {"bat": 4.5, "pit": 0.0}}}

        player = {
            "mlb_id": 998,
            "player_name": player_name,
            "age": 25,
            "fwar": 4.5,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 10.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }

        result = self._compute(player, war_history, [1.0])

        mr = result["raw_components"]["projection"]["mean_reversion"]
        projection = result["raw_components"]["projection"]

        self.assertTrue(mr["mean_reversion_applied"])
        self.assertLess(mr["regressed_fwar"], mr["base_fwar"])
        self.assertEqual(mr["mean_revert_target_war"], 2.5)
        self.assertEqual(mr["mean_revert_weight"], 0.35)
        self.assertEqual(mr["player_age"], 25)
        self.assertEqual(mr["age_threshold"], 26)

        self.assertEqual(projection["base_fwar"], 4.5)
        self.assertEqual(projection["fwar_used_for_projection"], mr["regressed_fwar"])

    def test_pitchers_skip_mean_reversion(self) -> None:
        """Verify pitchers don't get mean reversion."""
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year

        player_name = "Pitcher No Mean Revert"
        name_key = normalize_name(player_name)

        war_history = {name_key: {snapshot_year: {"bat": 0.0, "pit": 4.0}}}

        player = {
            "mlb_id": 997,
            "player_name": player_name,
            "age": 24,
            "fwar": 4.0,
            "contract": {
                "contract_years": [
                    {"season": snapshot_year, "salary_m": 10.0, "is_guaranteed": True}
                ],
                "options": [],
                "aav_m": None,
                "years_remaining": 0,
            },
        }

        result = self._compute(player, war_history, [1.0], pitcher_names={name_key})

        mr = result["raw_components"]["projection"]["mean_reversion"]

        self.assertFalse(mr["mean_reversion_applied"])
        self.assertEqual(mr["reason"], "pitcher")

    def test_pca_tvp_drops_after_improvements(self) -> None:
        """
        Integration test: Pete Crow-Armstrong TVP should drop >20M after
        control_fallback arb model + mean reversion.

        This test requires local data files. Skip gracefully if not available.
        """
        pca_path = Path(__file__).parent / "output" / "players_with_contracts_2025.json"
        if not pca_path.exists():
            self.skipTest("Pete Crow-Armstrong data not available for integration test")

        all_players = load_players(pca_path)
        pca_player = next(
            (p for p in all_players.get("players", []) if p.get("mlb_id") == 691718),
            None,
        )

        if not pca_player:
            self.skipTest("Pete Crow-Armstrong not found in players file")

        name_key = normalize_name(pca_player["player_name"])
        war_history = {name_key: {2025: {"bat": 4.5, "pit": 0.0}}}

        result = self._compute(pca_player, war_history, [1.0])
        tvp_current = result["tvp_mlb"]

        rc = result["raw_components"]

        self.assertTrue(rc["quality_flags"]["control_fallback_used"])

        mr = rc["projection"]["mean_reversion"]
        self.assertTrue(mr["mean_reversion_applied"])
        self.assertLess(mr["regressed_fwar"], mr["base_fwar"])

        control_fb = rc["contract"].get("control_fallback", {})
        self.assertIn("salary_components_by_t", control_fb)

        self.assertIsNotNone(tvp_current)
        self.assertGreater(tvp_current, 0)

    def _load_players(self) -> dict:
        """Helper to load players from JSON file."""
        repo_root = Path(__file__).resolve().parent
        path = repo_root / "output" / "players_with_contracts_2025.json"
        with path.open("r") as handle:
            return json.load(handle)

    if __name__ == "__main__":
        unittest.main()
