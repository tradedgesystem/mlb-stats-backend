import math
import unittest
from pathlib import Path

from compute_mlb_tvp import compute_player_tvp, normalize_name
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
        result = compute_player_tvp(
            player,
            snapshot_year,
            config_path,
            max_years=10,
            fwar_scale=0.70,
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
        return result, config

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
        result, _ = self._compute(player, war_history, [0.5, 0.3, 0.2])
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
                    {"season": snapshot_year + 1, "salary_m": 6.0, "is_guaranteed": True},
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
        result, _ = self._compute(player, war_history, [1.0])
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


if __name__ == "__main__":
    unittest.main()
