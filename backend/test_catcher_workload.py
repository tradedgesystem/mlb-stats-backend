import pytest
pytest.skip('Deprecated: MLB TVP v1 migration', allow_module_level=True)

#!/usr/bin/env python3
"""Unit tests for catcher workload building."""

import json
import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend"))

from build_catcher_workload import (
    parse_innings,
    is_catcher_position,
    fetch_catcher_stats_batch,
)


class TestInningsParsing(unittest.TestCase):
    def test_parse_innings_full_inning(self):
        self.assertEqual(parse_innings("44"), 44.0)
        self.assertEqual(parse_innings("100"), 100.0)
        self.assertEqual(parse_innings("0"), 0.0)

    def test_parse_innings_third(self):
        self.assertAlmostEqual(parse_innings("44.1"), 44.3333, places=4)
        self.assertAlmostEqual(parse_innings("0.1"), 0.3333, places=4)
        self.assertAlmostEqual(parse_innings("100.1"), 100.3333, places=4)

    def test_parse_innings_two_thirds(self):
        self.assertAlmostEqual(parse_innings("44.2"), 44.6667, places=4)
        self.assertAlmostEqual(parse_innings("0.2"), 0.6667, places=4)
        self.assertAlmostEqual(parse_innings("100.2"), 100.6667, places=4)

    def test_parse_innings_none(self):
        self.assertEqual(parse_innings(None), 0.0)
        self.assertEqual(parse_innings(""), 0.0)
        self.assertEqual(parse_innings("invalid"), 0.0)


class TestCatcherShareCalculation(unittest.TestCase):
    def test_share_from_innings(self):
        """Catching share computed from innings when available."""
        mock_payload = {
            "people": [
                {
                    "id": 663728,
                    "stats": [
                        {
                            "group": {"displayName": "fielding"},
                            "splits": [
                                {
                                    "position": {"abbreviation": "C"},
                                    "stat": {"games": 100, "innings": "850"},
                                    "games": 100,
                                    "innings": "850",
                                },
                                {
                                    "position": {"abbreviation": "1B"},
                                    "stat": {"games": 10, "innings": "50"},
                                    "games": 10,
                                    "innings": "50",
                                },
                            ],
                        }
                    ],
                }
            ]
        }

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("json.load", return_value=mock_payload):
                results = fetch_catcher_stats_batch([663728], 2025)
                self.assertIn(663728, results)
                self.assertAlmostEqual(
                    results[663728]["catching_share"], 850 / (850 + 50), places=4
                )
                self.assertEqual(results[663728]["innings_catching"], 850.0)
                self.assertEqual(results[663728]["innings_total"], 900.0)
                self.assertEqual(results[663728]["season_used"], 2025)
                self.assertEqual(results[663728]["source"], "mlb_stats_api")

    def test_share_from_innings_with_thirds(self):
        """Innings with 1/3 and 2/3 parsed correctly."""
        mock_payload = {
            "people": [
                {
                    "id": 669257,
                    "stats": [
                        {
                            "group": {"displayName": "fielding"},
                            "splits": [
                                {
                                    "position": {"abbreviation": "C"},
                                    "stat": {"games": 100, "innings": "850.2"},
                                    "games": 100,
                                    "innings": "850.2",
                                },
                                {
                                    "position": {"abbreviation": "DH"},
                                    "stat": {"games": 10, "innings": "0"},
                                    "games": 10,
                                    "innings": "0",
                                },
                            ],
                        }
                    ],
                }
            ]
        }

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("json.load", return_value=mock_payload):
                results = fetch_catcher_stats_batch([669257], 2025)
                self.assertIn(669257, results)
                # 850.2 innings = 850 + 2/3 = 850.6667
                self.assertAlmostEqual(
                    results[669257]["innings_catching"], 850.667, places=3
                )
                self.assertAlmostEqual(results[669257]["catching_share"], 1.0, places=4)

    def test_share_fallback_to_games(self):
        """No innings available, use games."""
        mock_payload = {
            "people": [
                {
                    "id": 669257,
                    "stats": [
                        {
                            "group": {"displayName": "fielding"},
                            "splits": [
                                {
                                    "position": {"abbreviation": "C"},
                                    "stat": {"games": 90},
                                    "games": 90,
                                },
                                {
                                    "position": {"abbreviation": "DH"},
                                    "stat": {"games": 10},
                                    "games": 10,
                                },
                            ],
                        }
                    ],
                }
            ]
        }

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("json.load", return_value=mock_payload):
                results = fetch_catcher_stats_batch([669257], 2025)
                self.assertIn(669257, results)
                self.assertAlmostEqual(results[669257]["catching_share"], 0.9, places=4)
                self.assertEqual(results[669257]["games_catching"], 90)
                self.assertEqual(results[669257]["games_total"], 100)

    def test_zero_totals(self):
        """Total innings and games both zero -> share=0.0."""
        mock_payload = {
            "people": [
                {
                    "id": 123456,
                    "stats": [{"group": {"displayName": "fielding"}, "splits": []}],
                }
            ]
        }

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("json.load", return_value=mock_payload):
                results = fetch_catcher_stats_batch([123456], 2025)
                self.assertIn(123456, results)
                self.assertEqual(results[123456]["catching_share"], 0.0)
                # When fielding_stats exists but splits are empty, still mlb_stats_api
                self.assertEqual(results[123456]["source"], "mlb_stats_api")

    def test_no_fielding_stats(self):
        """Player has no fielding stats group."""
        mock_payload = {"people": [{"id": 789012, "stats": []}]}

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("json.load", return_value=mock_payload):
                results = fetch_catcher_stats_batch([789012], 2025)
                self.assertIn(789012, results)
                self.assertEqual(results[789012]["catching_share"], 0.0)
                self.assertEqual(results[789012]["source"], "no_stats")


class TestCatcherPositionDetection(unittest.TestCase):
    def test_is_catcher_position_single(self):
        self.assertTrue(is_catcher_position("C"))
        self.assertTrue(is_catcher_position("c"))

    def test_is_catcher_position_multi(self):
        self.assertTrue(is_catcher_position("C/1B"))
        self.assertTrue(is_catcher_position("1B/C"))
        self.assertTrue(is_catcher_position("C, 1B, DH"))
        self.assertTrue(is_catcher_position("C - 1B"))

    def test_is_catcher_position_false(self):
        self.assertFalse(is_catcher_position("1B"))
        self.assertFalse(is_catcher_position("SS"))
        self.assertFalse(is_catcher_position("OF"))
        self.assertFalse(is_catcher_position(None))
        self.assertFalse(is_catcher_position(""))
        self.assertFalse(is_catcher_position("  "))


class TestAPIParameters(unittest.TestCase):
    def test_api_params_include_season(self):
        """Season is passed as separate query param, not in hydrate."""
        mock_payload = {"people": []}

        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = mock_response

            with patch("json.load", return_value=mock_payload):
                fetch_catcher_stats_batch([663728], 2025)

                # Verify URL was called with correct params
                call_args = mock_urlopen.call_args[0][0]
                url_str = str(call_args)
                # Check for season parameter (URL-encoded)
                self.assertIn("season=2025", url_str)
                # Check hydrate contains fielding group and season type (URL-encoded)
                self.assertIn("fielding", url_str)
                self.assertIn("season", url_str)
                # Ensure season is not in hydrate string (should be separate param)
                # hydrate should have "group=[fielding]" not "group=[fielding],season=2025"
                import urllib.parse

                parsed = urllib.parse.urlparse(url_str)
                params = urllib.parse.parse_qs(parsed.query)
                hydrate_val = params.get("hydrate", [""])[0]
                # hydrate should not contain season parameter value
                self.assertNotIn("2025", hydrate_val)
                self.assertIn("fielding", hydrate_val)


if __name__ == "__main__":
    unittest.main()
