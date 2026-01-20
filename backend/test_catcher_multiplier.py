#!/usr/bin/env python3
"""
Regression tests for catcher multiplier (mlb_id-first).
"""

import unittest
from pathlib import Path
import sys
import tempfile


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend"))

import compute_mlb_tvp  # noqa: E402
from compute_mlb_tvp import compute_player_tvp  # noqa: E402
from tvp_engine import load_config  # noqa: E402


FIXTURE_PATH = REPO_ROOT / "backend" / "player_positions_fixture.json"


class TestCatcherMultiplierRegression(unittest.TestCase):
    def setUp(self):
        self.config_path = REPO_ROOT / "backend" / "tvp_config.json"
        self.snapshot_year = load_config(self.config_path).snapshot_year

    def _base_player(self, mlb_id: int, name: str) -> dict:
        return {
            "mlb_id": mlb_id,
            "player_name": name,
            "age": 28,
            "fwar": 5.0,
            "contract": {
                "contract_years": [
                    {
                        "season": self.snapshot_year + i,
                        "salary_m": 5.0,
                        "is_guaranteed": True,
                    }
                    for i in range(5)
                ],
                "options": [],
                "aav_m": 5.0,
                "years_remaining": 5,
            },
        }

    def _compute_tvp(
        self,
        player: dict,
        catcher_ids: set[int],
        positions_missing: bool = False,
    ) -> dict:
        return compute_player_tvp(
            player,
            self.snapshot_year,
            self.config_path,
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
            war_history={},
            fwar_weights=[0.5, 0.3, 0.2],
            fwar_weight_seasons=[self.snapshot_year - offset for offset in range(3)],
            pitcher_names=set(),
            pitcher_regress_weight=0.0,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
            catcher_ids=catcher_ids,
            positions_missing=positions_missing,
        )

    def test_synthetic_catcher_receives_haircut(self):
        catcher = self._base_player(999999, "Synthetic Catcher")
        catcher["position"] = "C"
        catcher["position_source"] = "test_fixture"
        catcher_ids = {catcher["mlb_id"]}

        result = self._compute_tvp(catcher, catcher_ids)
        projection = result.get("raw_components", {}).get("projection", {})

        self.assertTrue(projection.get("is_catcher"))
        catcher_risk_applied = projection.get("catcher_risk_applied")
        self.assertTrue(catcher_risk_applied)

        non_catcher = self._base_player(999998, "Synthetic Shortstop")
        non_catcher["position"] = "SS"
        non_catcher["position_source"] = "test_fixture"
        non_catcher_result = self._compute_tvp(non_catcher, set())

        catcher_tvp = result.get("tvp_mlb")
        non_catcher_tvp = non_catcher_result.get("tvp_mlb")
        self.assertIsNotNone(catcher_tvp)
        self.assertIsNotNone(non_catcher_tvp)
        self.assertLess(catcher_tvp, non_catcher_tvp)

    def test_synthetic_non_catcher_unchanged(self):
        non_catcher = self._base_player(999998, "Synthetic Shortstop")
        non_catcher["position"] = "SS"
        non_catcher["position_source"] = "test_fixture"

        result = self._compute_tvp(non_catcher, set())
        projection = result.get("raw_components", {}).get("projection", {})

        self.assertFalse(projection.get("is_catcher"))
        catcher_risk_applied = projection.get("catcher_risk_applied")
        self.assertFalse(catcher_risk_applied)

    def test_real_ids_from_fixture(self):
        if not FIXTURE_PATH.exists():
            self.fail(f"Missing fixture file: {FIXTURE_PATH}")

        positions_map = compute_mlb_tvp.load_player_positions_map(FIXTURE_PATH)
        position_by_id = {mlb_id: info for mlb_id, info in positions_map.items()}
        catcher_ids = compute_mlb_tvp.build_catcher_ids(position_by_id)

        self.assertIn(663728, catcher_ids, "Cal Raleigh should be catcher")
        self.assertIn(669257, catcher_ids, "Will Smith should be catcher")
        self.assertNotIn(672695, catcher_ids, "Geraldo Perdomo should not be catcher")

        # Spot-check catcher detection via compute_player_tvp
        cal = self._base_player(663728, "Cal Raleigh")
        cal["position"] = positions_map[663728]["position"]
        cal["position_source"] = positions_map[663728]["position_source"]
        cal_result = self._compute_tvp(cal, catcher_ids)
        cal_proj = cal_result.get("raw_components", {}).get("projection", {})
        self.assertTrue(cal_proj.get("is_catcher"))

    def test_missing_positions_hard_fails_without_allow(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            positions_path = Path(temp_dir) / "player_positions.json"
            players_path = Path(temp_dir) / "players.json"
            players_path.write_text('{"players": []}', encoding="utf-8")
            with self.assertRaises(RuntimeError) as ctx:
                compute_mlb_tvp.ensure_positions_map(
                    positions_path,
                    players_path,
                    allow_missing=False,
                    no_position_refresh=True,
                )
            self.assertIn("Missing positions map", str(ctx.exception))

    def test_missing_positions_marks_quality_flag(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            positions_path = Path(temp_dir) / "player_positions.json"
            players_path = Path(temp_dir) / "players.json"
            players_path.write_text('{"players": []}', encoding="utf-8")
            positions_map, positions_missing = compute_mlb_tvp.ensure_positions_map(
                positions_path,
                players_path,
                allow_missing=True,
                no_position_refresh=True,
            )
            self.assertEqual(positions_map, {})
            self.assertTrue(positions_missing)

        player = self._base_player(999997, "Missing Position Player")
        result = self._compute_tvp(player, set(), positions_missing=True)
        projection = result.get("raw_components", {}).get("projection", {})
        quality_flags = result.get("raw_components", {}).get("quality_flags", {})
        self.assertEqual(projection.get("position_source"), "missing")
        self.assertTrue(quality_flags.get("positions_missing"))


if __name__ == "__main__":
    unittest.main()
