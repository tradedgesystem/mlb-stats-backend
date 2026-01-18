import math
import unittest
from pathlib import Path

from compute_mlb_tvp import compute_player_tvp
from tvp_engine import load_config


class TestControlFallbackSalary(unittest.TestCase):
    def test_control_fallback_min_salary_floor(self) -> None:
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
                "aav_m": 0.75,
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

        contract = result["raw_components"]["contract"]
        self.assertEqual(contract["contract_source"], "control_fallback")
        salary_by_season = contract["salary_by_season"]
        self.assertTrue(salary_by_season)

        for season, salary in salary_by_season.items():
            t = season - snapshot_year
            min_salary = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
            self.assertTrue(
                math.isclose(salary, min_salary, rel_tol=0.0, abs_tol=1e-9),
                msg=f"season={season} salary={salary} min_salary={min_salary}",
            )


if __name__ == "__main__":
    unittest.main()
