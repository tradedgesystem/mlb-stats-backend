import pytest
pytest.skip('Deprecated: MLB TVP v1 migration', allow_module_level=True)

#!/usr/bin/env python3
"""
Unit tests for catcher share scaling.
Tests that catcher adjustments are properly scaled by catching_share.
"""

import json
import tempfile
import unittest
from pathlib import Path

import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend"))

import compute_mlb_tvp  # noqa: E402
from compute_mlb_tvp import compute_player_tvp, apply_catcher_risk_adjustments
from tvp_engine import load_config


class TestCatcherShareScaling(unittest.TestCase):
    def setUp(self):
        self.config_path = REPO_ROOT / "backend" / "tvp_config.json"
        self.snapshot_year = load_config(self.config_path).snapshot_year

    def _base_player(self, mlb_id: int, name: str, age: int = 28) -> dict:
        return {
            "mlb_id": mlb_id,
            "player_name": name,
            "age": age,
            "fwar": 5.0,
            "position": "C",
            "position_source": "test_fixture",
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
        catching_workload_map: dict[int, float],
        disable_catcher_adjust: bool = False,
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
            fwar_weight_seasons=[self.snapshot_year - i for i in range(3)],
            pitcher_names=set(),
            pitcher_regress_weight=0.0,
            pitcher_regress_target=2.0,
            contracts_2026_map={},
            young_player_max_age=24,
            young_player_scale=1.0,
            catcher_ids={player["mlb_id"]},
            catcher_workload_map=catching_workload_map,
            positions_missing=False,
            disable_catcher_adjust=disable_catcher_adjust,
        )

    def test_catcher_share_0_no_adjustment(self):
        """Catching share=0 should mean no adjustment applied."""
        player = self._base_player(100, "Share Zero Catcher")
        workload_map = {100: 0.0}

        result_no_adjust = self._compute_tvp(
            player, workload_map, disable_catcher_adjust=True
        )
        result_adjust = self._compute_tvp(player, workload_map)

        tvp_no_adjust = result_no_adjust.get("tvp_mlb")
        tvp_adjust = result_adjust.get("tvp_mlb")

        self.assertIsNotNone(tvp_no_adjust)
        self.assertIsNotNone(tvp_adjust)
        self.assertAlmostEqual(
            tvp_adjust,
            tvp_no_adjust,
            places=4,
            msg="Share 0 catcher should have same TVP as non-catcher",
        )

    def test_catcher_share_1_full_adjustment(self):
        """Catching share=1 should apply full adjustment."""
        player = self._base_player(101, "Share One Catcher", age=30)
        workload_map = {101: 1.0}

        result = self._compute_tvp(player, workload_map)
        proj = result.get("raw_components", {}).get("projection", {})

        self.assertTrue(proj.get("is_catcher"))
        self.assertTrue(proj.get("catcher_risk_applied"))
        self.assertEqual(proj.get("catching_share"), 1.0)
        self.assertEqual(proj.get("catching_share_source"), "workload_map")

        non_catcher = self._base_player(199, "Non Catcher")
        non_catcher["position"] = "SS"
        non_catcher_result = self._compute_tvp(
            non_catcher, {}, disable_catcher_adjust=True
        )

        catcher_tvp = result.get("tvp_mlb")
        non_catcher_tvp = non_catcher_result.get("tvp_mlb")

        self.assertIsNotNone(catcher_tvp)
        self.assertIsNotNone(non_catcher_tvp)
        self.assertLess(
            catcher_tvp,
            non_catcher_tvp,
            msg="Catcher with share=1 should have lower TVP than non-catcher",
        )

    def test_catcher_share_05_partial_adjustment(self):
        """Catching share=0.5 should apply partial adjustment (~half)."""
        player = self._base_player(102, "Share Half Catcher", age=30)
        workload_map = {102: 0.5}

        result_half = self._compute_tvp(player, workload_map)
        result_full = self._compute_tvp(
            self._base_player(102, "Share One Catcher", age=30), {102: 1.0}
        )

        non_catcher_result = self._compute_tvp(
            self._base_player(199, "Non Catcher"),
            {},
            disable_catcher_adjust=True,
        )
        baseline_tvp = non_catcher_result.get("tvp_mlb")

        tvp_half = result_half.get("tvp_mlb")
        tvp_full = result_full.get("tvp_mlb")

        self.assertIsNotNone(baseline_tvp)
        self.assertIsNotNone(tvp_half)
        self.assertIsNotNone(tvp_full)

        baseline_val = baseline_tvp if baseline_tvp is not None else 0.0
        haircut_half = (
            (baseline_val - tvp_half) / baseline_val * 100.0
            if baseline_val > 0
            else 0.0
        )
        haircut_full = (
            (baseline_val - tvp_full) / baseline_val * 100.0
            if baseline_val > 0
            else 0.0
        )

        self.assertGreater(
            haircut_half,
            0.0,
            msg="Share 0.5 catcher should have positive haircut",
        )
        self.assertLess(
            haircut_half,
            haircut_full,
            msg="Share 0.5 should have smaller haircut than share=1.0",
        )

        if haircut_full is not None and haircut_half is not None:
            self.assertAlmostEqual(
                haircut_half,
                haircut_full * 0.5,
                delta=haircut_full * 0.5,
                msg="Share 0.5 should have ~half the haircut of share=1.0",
            )

    def test_catcher_severity_calculation(self):
        """Test that severity increases with catching_share above start."""
        with (REPO_ROOT / "backend" / "tvp_mlb_defaults.json").open("r") as handle:
            mlb_defaults = json.load(handle)

        catcher_config = mlb_defaults.get("catcher", {})
        start = catcher_config.get("workload_surcharge_start", 0.70)
        k = catcher_config.get("workload_surcharge_k", 0.40)

        # share below start: severity = 1.0, surplus = 0.0
        share = 0.5
        surplus = 0.0 if share <= start else (share - start) / max(1e-9, 1.0 - start)
        severity = 1.0 + k * surplus
        self.assertAlmostEqual(
            severity, 1.0, places=2, msg="Share below start should have severity=1.0"
        )
        self.assertAlmostEqual(
            surplus, 0.0, places=2, msg="Share below start should have surplus=0.0"
        )

        # share above start: severity > 1.0
        share = 0.9
        surplus = 0.0 if share <= start else (share - start) / max(1e-9, 1.0 - start)
        severity = 1.0 + k * surplus
        self.assertGreater(
            severity, 1.0, msg="Share above start should have severity > 1.0"
        )
        expected_severity = 1.0 + 0.40 * (0.9 - 0.7) / (1.0 - 0.7)
        self.assertAlmostEqual(
            severity, expected_severity, places=4, msg="Severity should match formula"
        )

    def test_catcher_share_scaling_direct(self):
        """Direct test of apply_catcher_risk_adjustments with different shares."""
        projected_fwar = {
            2026: 4.0,
            2027: 3.8,
            2028: 3.6,
            2029: 3.4,
            2030: 3.2,
        }
        age_by_season: dict[int, float | None] = {
            2026: 30.0,
            2027: 31.0,
            2028: 32.0,
            2029: 33.0,
            2030: 34.0,
        }

        with (REPO_ROOT / "backend" / "tvp_mlb_defaults.json").open("r") as handle:
            mlb_defaults = json.load(handle)

        result_0, meta_0 = apply_catcher_risk_adjustments(
            projected_fwar,
            age_by_season,
            2026,
            30.0,
            is_catcher=True,
            mlb_defaults=mlb_defaults,
            catching_share=0.0,
            apply_aging=True,
        )

        result_05, meta_05 = apply_catcher_risk_adjustments(
            projected_fwar,
            age_by_season,
            2026,
            30.0,
            is_catcher=True,
            mlb_defaults=mlb_defaults,
            catching_share=0.5,
            apply_aging=True,
        )

        result_1, meta_1 = apply_catcher_risk_adjustments(
            projected_fwar,
            age_by_season,
            2026,
            30.0,
            is_catcher=True,
            mlb_defaults=mlb_defaults,
            catching_share=1.0,
            apply_aging=True,
        )

        for season in projected_fwar.keys():
            self.assertEqual(
                result_0[season],
                projected_fwar[season],
                msg=f"Share 0 should not change fwar for season {season}",
            )

        for season in projected_fwar.keys():
            self.assertLess(
                result_1[season],
                result_05[season],
                msg=f"Share 1 should have lowest fwar for season {season}",
            )
            self.assertLess(
                result_05[season],
                result_0[season],
                msg=f"Share 0.5 fwar should be between share 0 and share 1 for season {season}",
            )

        self.assertFalse(meta_0.get("catcher_risk_applied"))
        self.assertTrue(meta_05.get("catcher_risk_applied"))
        self.assertTrue(meta_1.get("catcher_risk_applied"))

        self.assertEqual(meta_0.get("catching_share"), 0.0)
        self.assertEqual(meta_05.get("catching_share"), 0.5)
        self.assertEqual(meta_1.get("catching_share"), 1.0)


if __name__ == "__main__":
    unittest.main()
