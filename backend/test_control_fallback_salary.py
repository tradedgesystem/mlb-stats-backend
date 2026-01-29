import pytest
pytest.skip('Deprecated: MLB TVP v1 migration', allow_module_level=True)

import math
import unittest
from pathlib import Path

from compute_mlb_tvp import compute_player_tvp
from tvp_engine import load_config


class TestControlFallbackSalary(unittest.TestCase):
    def _compute_for_aav(self, aav_m: float) -> tuple[dict, dict]:
        repo_root = Path(__file__).resolve().parent
        config_path = repo_root / "tvp_config.json"
        config = load_config(config_path)
        snapshot_year = config.snapshot_year
        player = {
            "mlb_id": 1,
            "player_name": "Test Player",
            "age": 23,
            "fwar": 1.0,
            "contract": {
                "contract_years": [],
                "options": [],
                "aav_m": aav_m,
                "years_remaining": 0,
            },
        }
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
            control_years_fallback=4,
            control_years_age_max=27,
            two_way_names=set(),
            two_way_fwar_cap=8.0,
            two_way_mult=1.5,
            war_history={},
            fwar_weights=[1.0],
            fwar_weight_seasons=[snapshot_year - 1],
            pitcher_names=set(),
            pitcher_regress_weight=0.35,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
        )
        return result, config

    def test_control_fallback_sub_min_clamps_up(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        aav_m = config.min_salary_m * 0.9
        result, config = self._compute_for_aav(aav_m)
        contract = result["raw_components"]["contract"]
        self.assertEqual(contract["contract_source"], "control_fallback")
        salary_by_season = contract["salary_by_season"]
        self.assertTrue(salary_by_season)
        self.assertEqual(
            sorted(contract["control_salary_floor_seasons"]),
            sorted(salary_by_season.keys()),
        )

        for season, salary in salary_by_season.items():
            t = season - config.snapshot_year
            min_salary = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
            self.assertTrue(
                math.isclose(salary, min_salary, rel_tol=0.0, abs_tol=1e-9),
                msg=f"season={season} salary={salary} min_salary={min_salary}",
            )

    def test_control_fallback_above_min_not_reduced(self) -> None:
        config_path = Path(__file__).resolve().parent / "tvp_config.json"
        config = load_config(config_path)
        aav_m = config.min_salary_m * 2.0
        result, config = self._compute_for_aav(aav_m)
        contract = result["raw_components"]["contract"]
        self.assertEqual(contract["contract_source"], "control_fallback")
        self.assertEqual(contract["control_salary_floor_seasons"], [])
        salary_by_season = contract["salary_by_season"]
        self.assertTrue(salary_by_season)

        for season, salary in salary_by_season.items():
            t = season - config.snapshot_year
            min_salary = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
            self.assertGreaterEqual(salary, min_salary)
            self.assertTrue(
                math.isclose(salary, aav_m, rel_tol=0.0, abs_tol=1e-9),
                msg=f"season={season} salary={salary} aav_m={aav_m}",
            )


if __name__ == "__main__":
    unittest.main()
