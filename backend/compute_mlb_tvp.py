from __future__ import annotations

import argparse
import json
import math
import os
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sqlite3
import re
import sys
import subprocess

from tvp_engine import compute_mlb_tvp, compute_rookie_alpha, load_config


SPECIAL_SALARY_OVERRIDES_M = {
    "Shohei Ohtani": 70.0,
}
LONG_CONTROL_BASELINE_YEARS = 4
LONG_CONTROL_AAV_MAX = 15.0
LONG_CONTROL_STEP_BOOST_PCT = 0.15
LONG_CONTROL_MAX_BOOST_PCT = 0.6


def adjust_player_age(player: dict[str, Any], age_offset: int) -> dict[str, Any]:
    if age_offset <= 0:
        return player
    age = player.get("age")
    if age is None:
        return player
    try:
        adjusted_age = age + age_offset
    except TypeError:
        return player
    if adjusted_age == age:
        return player
    updated = dict(player)
    updated["age"] = adjusted_age
    return updated


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKD", name)
    stripped = "".join(char for char in normalized if not unicodedata.combining(char))
    stripped = re.sub(r"\(.*?\)", "", stripped)
    stripped = stripped.replace(".", " ")
    stripped = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", stripped, flags=re.IGNORECASE)
    stripped = re.sub(r"[^a-zA-Z\s]", " ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip().lower()
    return re.sub(r"[^a-z]", "", stripped)


def load_reliever_names(season: int, db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name, g, gs, relief_ip, start_ip
        FROM pitching_stats
        WHERE season = ?
        """,
        (season,),
    )
    relievers: set[str] = set()
    for name, g, gs, relief_ip, start_ip in cursor.fetchall():
        if name is None:
            continue
        gs_value = gs or 0
        relief_ip = relief_ip or 0.0
        start_ip = start_ip or 0.0
        if gs_value <= 3 or relief_ip > start_ip:
            relievers.add(normalize_name(name))
    conn.close()
    return relievers


def load_pitcher_names(season: int, db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name, g, gs, relief_ip, start_ip
        FROM pitching_stats
        WHERE season = ?
        """,
        (season,),
    )
    pitchers: set[str] = set()
    for name, g, gs, relief_ip, start_ip in cursor.fetchall():
        if name is None:
            continue
        relief_ip = relief_ip or 0.0
        start_ip = start_ip or 0.0
        if (g or 0) > 0 and (relief_ip > 0 or start_ip > 0):
            pitchers.add(normalize_name(name))
    conn.close()
    return pitchers


def load_player_positions_map(positions_path: Path) -> dict[int, dict[str, str | None]]:
    """Load mlb_id-first position mapping file."""
    if not positions_path.exists():
        return {}
    with positions_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    positions: dict[int, dict[str, str | None]] = {}
    if not isinstance(data, dict):
        return positions
    for key, value in data.items():
        try:
            mlb_id = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        positions[mlb_id] = {
            "position": value.get("position"),
            "position_source": value.get("position_source"),
        }
    return positions


def build_positions_map(positions_path: Path, players_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "build_player_positions.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--players",
        str(players_path),
        "--output",
        str(positions_path),
    ]
    subprocess.run(cmd, check=True)


def ensure_positions_map(
    positions_path: Path,
    players_path: Path,
    allow_missing: bool,
    no_position_refresh: bool,
) -> tuple[dict[int, dict[str, str | None]], bool]:
    positions_missing = False
    if not positions_path.exists():
        if no_position_refresh:
            if allow_missing:
                positions_missing = True
                print(
                    "WARNING: positions map missing; proceeding with catcher "
                    "detection disabled (--allow-missing-positions).",
                    file=sys.stderr,
                )
                return {}, positions_missing
            raise RuntimeError(
                f"Missing positions map: {positions_path}. "
                "Run scripts/build_player_positions.py or pass "
                "--allow-missing-positions."
            )
        try:
            print(
                f"Positions map missing; building via {positions_path}.",
                file=sys.stderr,
            )
            build_positions_map(positions_path, players_path)
        except subprocess.CalledProcessError as exc:
            if allow_missing:
                positions_missing = True
                print(
                    "WARNING: failed to build positions map; proceeding with "
                    "catcher detection disabled (--allow-missing-positions).",
                    file=sys.stderr,
                )
                return {}, positions_missing
            raise RuntimeError(
                "Failed to build positions map. Re-run with "
                "--allow-missing-positions or check network access."
            ) from exc

    positions_map = load_player_positions_map(positions_path)
    if not positions_map:
        if allow_missing:
            positions_missing = True
            print(
                "WARNING: positions map empty; proceeding with catcher "
                "detection disabled (--allow-missing-positions).",
                file=sys.stderr,
            )
            return {}, positions_missing
        raise RuntimeError(
            f"Positions map is empty: {positions_path}. "
            "Rebuild with scripts/build_player_positions.py or pass "
            "--allow-missing-positions."
        )
    return positions_map, positions_missing


def tokenize_position(position: str | None) -> list[str]:
    if not position:
        return []
    tokens = re.split(r"[\s,/]+", str(position).strip())
    return [token for token in tokens if token]


def is_catcher_position(position: str | None) -> bool:
    return any(token.upper() == "C" for token in tokenize_position(position))


def attach_positions(
    players: list[dict[str, Any]],
    positions_map: dict[int, dict[str, str | None]],
) -> dict[int, dict[str, str | None]]:
    """Attach position + source to players and return mlb_id->position info."""
    position_by_id: dict[int, dict[str, str | None]] = {}
    for player in players:
        mlb_id = player.get("mlb_id")
        if not isinstance(mlb_id, int):
            continue
        map_info = positions_map.get(mlb_id, {})
        position = map_info.get("position") or player.get("position")
        position_source = map_info.get("position_source") or player.get(
            "position_source"
        )
        if position is not None:
            player["position"] = position
        if position_source is not None:
            player["position_source"] = position_source
        position_by_id[mlb_id] = {
            "position": position,
            "position_source": position_source,
        }
    return position_by_id


def build_catcher_ids(position_by_id: dict[int, dict[str, str | None]]) -> set[int]:
    return {
        mlb_id
        for mlb_id, info in position_by_id.items()
        if is_catcher_position(info.get("position"))
    }


def parse_contract_years(contract_text: str | None, snapshot_year: int) -> list[int]:
    if not contract_text:
        return []
    text = contract_text.replace("\u2013", "-")
    match = re.search(r"(\d{4})\s*-\s*(\d{2,4})", text)
    if match:
        start = int(match.group(1))
        end_raw = match.group(2)
        if len(end_raw) == 2:
            end = (start // 100) * 100 + int(end_raw)
        else:
            end = int(end_raw)
        if end < start:
            return []
        return list(range(start, end + 1))
    term_match = re.search(r"(\d+)\s*yr", text, re.IGNORECASE)
    if term_match:
        term = int(term_match.group(1))
        return list(range(snapshot_year, snapshot_year + term))
    return []


def parse_aav_m(aav_text: str | None) -> float | None:
    if not aav_text:
        return None
    cleaned = aav_text.replace("$", "").replace(",", "").strip()
    if cleaned.endswith("M"):
        cleaned = cleaned[:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def load_contracts_2026_map(
    db_path: Path, snapshot_year: int
) -> dict[str, dict[str, Any]]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT team, player, contract, aav, aav_usd, source_pdf
        FROM contracts_2026
        """
    )
    contracts: dict[str, dict[str, Any]] = {}
    for team, player, contract, aav, aav_usd, source_pdf in cursor.fetchall():
        if not player:
            continue
        name_key = normalize_name(player)
        seasons = parse_contract_years(contract, snapshot_year)
        if not seasons:
            continue
        aav_m = None
        if aav_usd is not None:
            aav_m = float(aav_usd) / 1_000_000.0
        if aav_m is None:
            aav_m = parse_aav_m(aav)
        if aav_m is None:
            continue
        existing = contracts.get(name_key)
        if existing and len(existing.get("seasons", [])) >= len(seasons):
            continue
        contracts[name_key] = {
            "player_name": player,
            "team": team,
            "seasons": seasons,
            "salary_by_season": {season: aav_m for season in seasons},
            "aav_m": aav_m,
            "contract_text": contract,
            "source_pdf": source_pdf,
        }
    conn.close()
    return contracts


def load_two_way_names(season: int, db_path: Path, min_war: float) -> set[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name, war
        FROM batting_stats
        WHERE season = ?
        """,
        (season,),
    )
    batting: dict[str, float] = {}
    for name, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        batting[key] = max(batting.get(key, float("-inf")), float(war))

    cursor.execute(
        """
        SELECT name, war
        FROM pitching_stats
        WHERE season = ?
        """,
        (season,),
    )
    pitching: dict[str, float] = {}
    for name, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        pitching[key] = max(pitching.get(key, float("-inf")), float(war))

    conn.close()

    two_way = set()
    for key, bat_war in batting.items():
        if bat_war >= min_war and pitching.get(key, float("-inf")) >= min_war:
            two_way.add(key)
    # Enforce Ohtani-only qualification for now.
    ohtani_key = normalize_name("Shohei Ohtani")
    return {ohtani_key} if ohtani_key in two_way else set()


def load_war_history(
    seasons: list[int], db_path: Path
) -> dict[int | str, dict[int, dict[str, float]]]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    placeholders = ",".join("?" for _ in seasons)
    params = tuple(seasons)
    batting_by_name: dict[str, dict[int, float]] = {}
    pitching_by_name: dict[str, dict[int, float]] = {}
    batting_by_id: dict[int, dict[int, float]] = {}
    pitching_by_id: dict[int, dict[int, float]] = {}

    cursor.execute(
        f"""
        SELECT player_id, name, season, war
        FROM batting_stats
        WHERE season IN ({placeholders})
        """,
        params,
    )
    for player_id, name, season, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        batting_by_name.setdefault(key, {})
        batting_by_name[key][season] = batting_by_name[key].get(season, 0.0) + float(
            war
        )
        if player_id is not None:
            player_key = int(player_id)
            batting_by_id.setdefault(player_key, {})
            batting_by_id[player_key][season] = batting_by_id[player_key].get(
                season, 0.0
            ) + float(war)

    cursor.execute(
        f"""
        SELECT player_id, name, season, war
        FROM pitching_stats
        WHERE season IN ({placeholders})
        """,
        params,
    )
    for player_id, name, season, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        pitching_by_name.setdefault(key, {})
        pitching_by_name[key][season] = pitching_by_name[key].get(season, 0.0) + float(
            war
        )
        if player_id is not None:
            player_key = int(player_id)
            pitching_by_id.setdefault(player_key, {})
            pitching_by_id[player_key][season] = pitching_by_id[player_key].get(
                season, 0.0
            ) + float(war)

    conn.close()

    history: dict[int | str, dict[int, dict[str, float]]] = {}
    for key in set(batting_by_name) | set(pitching_by_name):
        history[key] = {}
        for season in seasons:
            bat_seasons = batting_by_name.get(key, {})
            pit_seasons = pitching_by_name.get(key, {})
            if season not in bat_seasons and season not in pit_seasons:
                continue
            bat = bat_seasons.get(season, 0.0)
            pit = pit_seasons.get(season, 0.0)
            history[key][season] = {"bat": bat, "pit": pit}
    for key in set(batting_by_id) | set(pitching_by_id):
        history[key] = {}
        for season in seasons:
            bat_seasons = batting_by_id.get(key, {})
            pit_seasons = pitching_by_id.get(key, {})
            if season not in bat_seasons and season not in pit_seasons:
                continue
            bat = bat_seasons.get(season, 0.0)
            pit = pit_seasons.get(season, 0.0)
            history[key][season] = {"bat": bat, "pit": pit}
    return history


def load_sample_counts(
    season: int, db_path: Path
) -> dict[str, dict[str, float | None]]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    batting_by_name: dict[str, dict[str, float | None]] = {}
    pitching_by_name: dict[str, dict[str, float | None]] = {}
    batting_by_id: dict[int, dict[str, float | None]] = {}
    pitching_by_id: dict[int, dict[str, float | None]] = {}

    cursor.execute(
        """
        SELECT player_id, name, pa, war
        FROM batting_stats
        WHERE season = ?
        """,
        (season,),
    )
    for player_id, name, pa, war in cursor.fetchall():
        if name is None:
            continue
        key = normalize_name(name)
        batting_by_name[key] = {
            "pa": float(pa) if pa is not None else None,
            "bat_war": float(war) if war is not None else None,
        }
        if player_id is not None:
            batting_by_id[int(player_id)] = {
                "pa": float(pa) if pa is not None else None,
                "bat_war": float(war) if war is not None else None,
            }

    cursor.execute(
        """
        SELECT player_id, name, ip, war
        FROM pitching_stats
        WHERE season = ?
        """,
        (season,),
    )
    for player_id, name, ip, war in cursor.fetchall():
        if name is None:
            continue
        key = normalize_name(name)
        pitching_by_name[key] = {
            "ip": float(ip) if ip is not None else None,
            "pit_war": float(war) if war is not None else None,
        }
        if player_id is not None:
            pitching_by_id[int(player_id)] = {
                "ip": float(ip) if ip is not None else None,
                "pit_war": float(war) if war is not None else None,
            }

    conn.close()

    sample_counts: dict[int | str, dict[str, float | None]] = {}
    for key in set(batting_by_name) | set(pitching_by_name):
        entry: dict[str, float | None] = {}
        entry.update(batting_by_name.get(key, {}))
        entry.update(pitching_by_name.get(key, {}))
        sample_counts[key] = entry
    for key in set(batting_by_id) | set(pitching_by_id):
        entry: dict[str, float | None] = {}
        entry.update(batting_by_id.get(key, {}))
        entry.update(pitching_by_id.get(key, {}))
        sample_counts[key] = entry
    return sample_counts


def load_prospect_anchors(
    repo_root: Path,
) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    output_dir = repo_root / "backend" / "output"
    candidates = sorted(output_dir.glob("tvp_prospects_*.json"))
    preferred = output_dir / "tvp_prospects_2026_final.json"
    if preferred.exists() and preferred not in candidates:
        candidates.append(preferred)
    if not candidates:
        return {}, {}
    anchor_path = candidates[-1]
    with anchor_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    by_mlb_id: dict[int, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for prospect in data.get("prospects", []):
        tvp_prospect = prospect.get("tvp_prospect")
        raw = prospect.get("raw_components") or {}
        fv_value = raw.get("fv_value")
        if tvp_prospect is None or fv_value is None:
            continue
        anchor = {
            "tvp_prospect": float(tvp_prospect),
            "fv_value": int(fv_value),
            "source_file": anchor_path.name,
            "raw_components": raw,
        }
        mlb_id = prospect.get("mlb_id")
        if isinstance(mlb_id, int):
            by_mlb_id[mlb_id] = anchor
        name_key = normalize_name(prospect.get("player_name"))
        if name_key:
            by_name[name_key] = anchor
    return by_mlb_id, by_name


def enrich_players(
    players: list[dict[str, Any]],
    sample_counts: dict[int | str, dict[str, float | None]],
    prospects_by_id: dict[int, dict[str, Any]],
    prospects_by_name: dict[str, dict[str, Any]],
) -> None:
    for player in players:
        sample = None
        mlb_id = player.get("mlb_id")
        if isinstance(mlb_id, int):
            sample = sample_counts.get(mlb_id)
        name_key = normalize_name(player.get("player_name"))
        if sample is None:
            sample = sample_counts.get(name_key, {})
        for field in ("pa", "ip", "bat_war", "pit_war"):
            if field in sample and player.get(field) is None:
                player[field] = sample.get(field)
        prospect_anchor = None
        if isinstance(mlb_id, int):
            prospect_anchor = prospects_by_id.get(mlb_id)
        if prospect_anchor is None and name_key:
            prospect_anchor = prospects_by_name.get(name_key)
        if prospect_anchor and player.get("prospect_anchor") is None:
            player["prospect_anchor"] = prospect_anchor


def compute_weighted_fwar(
    name_key: str,
    seasons: list[int],
    weights: list[float],
    war_history: dict[int | str, dict[int, dict[str, float]]],
    mlb_id: int | None = None,
) -> tuple[float | None, dict[str, Any]]:
    history_key: int | str | None = None
    if isinstance(mlb_id, int) and mlb_id in war_history:
        history_key = mlb_id
    elif name_key in war_history:
        history_key = name_key
    if history_key is None:
        return None, {"source": "missing_history", "seasons": seasons}
    season_war = []
    for season, weight in zip(seasons, weights):
        entry = war_history.get(history_key, {}).get(season)
        if entry is None:
            continue
        total = entry.get("bat", 0.0) + entry.get("pit", 0.0)
        season_war.append((season, weight, total, entry))
    if not season_war:
        return None, {"source": "missing_history", "seasons": seasons}
    used_weights = sum(item[1] for item in season_war)
    normalized = [
        (season, weight / used_weights, total, entry)
        for season, weight, total, entry in season_war
    ]
    weighted = sum(item[1] * item[2] for item in normalized)
    return weighted, {
        "source": "weighted_history",
        "history_key": history_key,
        "seasons": [
            {
                "season": season,
                "weight": weight,
                "war": total,
                "bat_war": entry.get("bat", 0.0),
                "pit_war": entry.get("pit", 0.0),
            }
            for season, weight, total, entry in normalized
        ],
        "seasons_count": len(normalized),
        "weights_sum": 1.0,
    }


def load_players(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def project_fwar_flat(
    base_fwar: float,
    seasons: list[int],
    scale: float,
    cap: float | None,
    age: float | None,
    snapshot_year: int,
    apply_aging: bool,
    prime_age: int,
    decline_per_year: float,
    floor: float,
    player_name: str | None = None,
    position: str | None = None,
    debug: bool = False,
) -> tuple[dict[int, float], dict[int, float | None], dict[int, float]]:
    projected: dict[int, float] = {}
    ages: dict[int, float | None] = {}
    aging_mults: dict[int, float] = {}
    debug_aging = os.getenv("DEBUG_AGING") or debug
    sample_names = os.getenv("DEBUG_AGING_SAMPLE", "")
    should_debug = (
        debug_aging
        and sample_names
        and player_name
        and player_name.lower() in sample_names.lower().split(",")
    )

    if should_debug:
        print(
            f"\n=== DEBUG AGING: {player_name} (position={position}, base_age={age}) ==="
        )
        print(
            f"apply_aging={apply_aging}, prime_age={prime_age}, decline_per_year={decline_per_year}, floor={floor}"
        )
        print(f"base_fwar={base_fwar:.3f}, scale={scale}, cap={cap}")
        print(
            f"{'Season':<10} {'Age':<6} {'Mult':<10} {'Pre-Aging WAR':<20} {'Post-Aging WAR':<20}"
        )
        print("-" * 80)

    for season in seasons:
        season_age = age + (season - snapshot_year) if age is not None else None
        mult = 1.0
        if apply_aging and season_age is not None and season_age > prime_age:
            mult = max(floor, 1.0 - decline_per_year * (season_age - prime_age))
        pre_aging_fwar = base_fwar * scale
        fwar = pre_aging_fwar * mult
        if cap is not None and fwar > cap:
            fwar = cap
        projected[season] = fwar
        ages[season] = season_age
        aging_mults[season] = mult

        if should_debug:
            print(
                f"{season:<10} {season_age if season_age else 'N/A':<6} {mult:<10.4f} {pre_aging_fwar:<20.4f} {fwar:<20.4f}"
            )

    if should_debug:
        print("=" * 80 + "\n")

    return projected, ages, aging_mults


def apply_catcher_risk_adjustments(
    projected_fwar: dict[int, float],
    age_by_season: dict[int, float | None],
    snapshot_year: int,
    player_age: float | None,
    is_catcher: bool,
    mlb_defaults: dict[str, Any],
    player_name: str | None = None,
    base_aging_mults: dict[int, float] | None = None,
    apply_aging: bool = True,
    prime_age: int = 29,
    decline_per_year: float = 0.035,
    aging_floor: float = 0.65,
) -> tuple[dict[int, float], dict[str, Any]]:
    """
    Apply catcher-specific risk adjustments including:
    - Availability playing-time reduction (catchers play fewer games)
    - Steeper aging decline (catchers wear down faster) - REPLACES base aging, not compounds
    - Position change risk premium (small TVP discount for C → 1B/DH risk)

    Returns:
        (adjusted_fwar_by_season, adjustment_meta)
    """
    adjustment_meta: dict[str, Any] = {
        "catcher_risk_applied": False,
        "adjustments": {},
        "base_aging_factor_by_t": {},
        "catcher_aging_factor_by_t": {},
        "final_aging_factor_by_t": {},
    }

    if not is_catcher or not player_age:
        if base_aging_mults:
            for t, mult in base_aging_mults.items():
                adjustment_meta["base_aging_factor_by_t"][t] = mult
        return projected_fwar, adjustment_meta

    catcher_config = mlb_defaults.get("catcher", {})

    playing_time_factor = catcher_config.get("playing_time_factor", 1.0)
    steeper_decline_age = catcher_config.get("steeper_decline_age", 28)
    steeper_decline_rate = catcher_config.get("steeper_decline_rate", 0.04)
    steeper_decline_floor = catcher_config.get("steeper_decline_floor", 0.55)
    position_change_age = catcher_config.get("position_change_age", 32)
    position_change_rate = catcher_config.get("position_change_rate", 0.03)

    adjusted_fwar: dict[int, float] = {}

    for season, base_fwar in sorted(projected_fwar.items()):
        season_age = age_by_season.get(season)
        t = season - snapshot_year
        if season_age is None:
            adjusted_fwar[season] = base_fwar
            continue

        # Track base aging factor for transparency
        if base_aging_mults and t in base_aging_mults:
            adjustment_meta["base_aging_factor_by_t"][t] = base_aging_mults[t]

        # Apply playing-time reduction (availability)
        fwar_after_playing_time = base_fwar * playing_time_factor

        # Apply catcher-specific aging (REPLACES, doesn't compound with base aging)
        if apply_aging and season_age >= steeper_decline_age:
            years_past_threshold = season_age - steeper_decline_age
            catcher_aging_mult = max(
                steeper_decline_floor,
                1.0 - (steeper_decline_rate * years_past_threshold),
            )
        else:
            catcher_aging_mult = 1.0

        # Track catcher aging factor
        adjustment_meta["catcher_aging_factor_by_t"][t] = catcher_aging_mult

        # Calculate final aging factor (for transparency)
        if apply_aging:
            # For catchers, use catcher_aging_mult directly (not compounded)
            final_aging_mult = catcher_aging_mult
        else:
            final_aging_mult = 1.0
        adjustment_meta["final_aging_factor_by_t"][t] = final_aging_mult

        # Apply position change risk premium (small discount for position-change risk)
        # This is applied as a TVP risk discount, not a WAR haircut
        position_change_prob = 0.0
        if season_age >= position_change_age:
            years_past_threshold = season_age - position_change_age + 1
            position_change_prob = min(1.0, position_change_rate * years_past_threshold)

        # Position change reduces final value by risk probability
        fwar_adjusted = (
            fwar_after_playing_time * catcher_aging_mult * (1.0 - position_change_prob)
        )

        adjusted_fwar[season] = fwar_adjusted

        # Store adjustment details
        adjustment_meta["adjustments"][season] = {
            "base_fwar": base_fwar,
            "playing_time_factor": playing_time_factor,
            "fwar_after_playing_time": fwar_after_playing_time,
            "catcher_aging_mult": catcher_aging_mult,
            "position_change_prob": position_change_prob,
            "final_fwar": fwar_adjusted,
        }

    adjustment_meta["catcher_risk_applied"] = True
    adjustment_meta["config"] = {
        "playing_time_factor": playing_time_factor,
        "steeper_decline_age": steeper_decline_age,
        "steeper_decline_rate": steeper_decline_rate,
        "steeper_decline_floor": steeper_decline_floor,
        "position_change_age": position_change_age,
        "position_change_rate": position_change_rate,
    }

    return adjusted_fwar, adjustment_meta


def build_salary_map(
    contract_years: list[dict[str, Any]], snapshot_year: int
) -> tuple[dict[int, float | None], set[int]]:
    salary_by_season: dict[int, float | None] = {}
    missing_salary_seasons: set[int] = set()
    for entry in contract_years:
        season = entry.get("season")
        if not isinstance(season, int) or season < snapshot_year:
            continue
        if entry.get("is_guaranteed", True) is False:
            continue
        salary = entry.get("salary_m")
        if salary is None:
            salary_by_season.setdefault(season, None)
            missing_salary_seasons.add(season)
            continue
        salary_value = float(salary)
        existing = salary_by_season.get(season)
        if existing is None:
            salary_by_season[season] = salary_value
        else:
            salary_by_season[season] = max(existing, salary_value)
    return salary_by_season, missing_salary_seasons


def compute_arb_salary(
    projected_value: float,
    control_year_index: int,
    min_salary: float,
    arb_shares: list[float],
) -> float:
    """
    Compute arbitration salary using ARB_SHARE from tvp_config.json.

    Years 1-3: Pre-arbitration, use minimum salary.
    Years 4-6: ARB_SHARE[y-1] of market value.

    Args:
        projected_value: WAR * price(t)
        control_year_index: 0-based index (1-6 for arb years)
        min_salary: MIN_SALARY_M * (1 + growth)^t
        arb_shares: ARB_SHARE values from tvp_config.json

    Returns:
        max(min_salary, arb_share * projected_value)
    """
    if control_year_index < 0 or control_year_index >= len(arb_shares):
        arb_share = arb_shares[-1]
    else:
        arb_share = arb_shares[control_year_index]

    arb_salary = arb_share * projected_value
    return max(min_salary, arb_salary)


def apply_control_year_fallback(
    seasons: list[int],
    salary_by_season: dict[int, float | None],
    missing_salary_seasons: set[int],
    snapshot_year: int,
    age: float | None,
    control_years: int,
    age_max: int,
    config: Any,
) -> tuple[list[int], dict[int, float | None], set[int], bool]:
    if control_years <= 0 or age is None or age > age_max:
        return seasons, salary_by_season, missing_salary_seasons, False
    if len(seasons) >= control_years:
        return seasons, salary_by_season, missing_salary_seasons, False
    start_year = min(seasons) if seasons else snapshot_year
    fallback_years = list(range(start_year, start_year + control_years))
    for year in fallback_years:
        salary_by_season.setdefault(year, None)
        missing_salary_seasons.add(year)

    return (
        fallback_years,
        salary_by_season,
        missing_salary_seasons,
        True,
    )


def apply_mean_reversion(
    base_fwar: float,
    is_hitter: bool,
    player_age: int | None,
    mlb_defaults: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    """
    Apply regression-to-mean for young hitters (age ≤ 26).

    Args:
        base_fwar: Selected fWAR (history_weighted or snapshot)
        is_hitter: False for pitchers (skip regression)
        player_age: Player age at snapshot
        mlb_defaults: tvp_mlb_defaults.json configuration

    Returns:
        (regressed_fwar, regression_meta dict)
    """
    mean_revert_params = mlb_defaults.get("hitter_mean_reversion", {})
    mean_revert_age_max = mean_revert_params.get("age_max", 26)
    mean_revert_target_war = mean_revert_params.get("target_war", 2.5)
    mean_revert_weight = mean_revert_params.get("weight", 0.35)

    if not is_hitter:
        return base_fwar, {"mean_reversion_applied": False, "reason": "pitcher"}

    if player_age is None or player_age > mean_revert_age_max:
        return base_fwar, {
            "mean_reversion_applied": False,
            "reason": f"age>{mean_revert_age_max}",
        }

    regressed_fwar = (
        1.0 - mean_revert_weight
    ) * base_fwar + mean_revert_weight * mean_revert_target_war

    regression_meta = {
        "mean_reversion_applied": True,
        "base_fwar": base_fwar,
        "regressed_fwar": regressed_fwar,
        "mean_revert_target_war": mean_revert_target_war,
        "mean_revert_weight": mean_revert_weight,
        "player_age": player_age,
        "age_threshold": mean_revert_age_max,
    }

    return regressed_fwar, regression_meta


def build_option_years(
    options: list[dict[str, Any]],
    snapshot_year: int,
    projected_fwar: dict[int, float],
) -> tuple[list[dict[str, Any]], set[int]]:
    option_entries: list[dict[str, Any]] = []
    option_seasons: set[int] = set()
    for opt in options:
        season = opt.get("season")
        if not isinstance(season, int) or season < snapshot_year:
            continue
        salary = opt.get("salary_m")
        if salary is None:
            continue
        option_seasons.add(season)
        option_entries.append(
            {
                "t": season - snapshot_year,
                "option_type": str(opt.get("type") or "").upper(),
                "option_salary_m": float(salary),
                "buyout_m": float(opt.get("buyout_m") or 0.0),
                "fwar": projected_fwar.get(season, 0.0),
            }
        )
    return option_entries, option_seasons


def compute_player_tvp(
    player: dict[str, Any],
    snapshot_year: int,
    config_path: Path,
    max_years: int,
    fwar_scale: float,
    fwar_cap: float | None,
    apply_aging: bool,
    prime_age: int,
    decline_per_year: float,
    aging_floor: float,
    reliever_names: set[str],
    reliever_mult: float,
    control_years_fallback: int,
    control_years_age_max: int,
    two_way_names: set[str],
    two_way_fwar_cap: float | None,
    two_way_mult: float,
    war_history: dict[str, dict[int, dict[str, float]]],
    fwar_weights: list[float],
    fwar_weight_seasons: list[int],
    pitcher_names: set[str],
    pitcher_regress_weight: float,
    pitcher_regress_target: float,
    contracts_2026_map: dict[str, dict[str, Any]],
    young_player_max_age: int,
    young_player_scale: float,
    catcher_ids: set[int] | None = None,
    positions_missing: bool = False,
    disable_catcher_adjust: bool = False,
) -> dict[str, Any]:
    config = load_config(config_path)

    repo_root = Path(__file__).resolve().parent
    mlb_defaults_path = repo_root / "tvp_mlb_defaults.json"
    with mlb_defaults_path.open("r") as handle:
        mlb_defaults = json.load(handle)
    base_fwar = player.get("fwar")
    if base_fwar is None:
        base_fwar = 0.0
    else:
        base_fwar = float(base_fwar)
    name_key = normalize_name(player.get("player_name"))
    is_reliever = name_key in reliever_names
    is_two_way = name_key in two_way_names
    mlb_id = player.get("mlb_id")
    if catcher_ids is None:
        catcher_ids = set()
    is_catcher = isinstance(mlb_id, int) and mlb_id in catcher_ids
    position = player.get("position")
    position_source = player.get("position_source")
    if positions_missing:
        position_source = "missing"
    player_age = player.get("age")

    repo_root = Path(__file__).resolve().parent
    mlb_defaults_path = repo_root / "tvp_mlb_defaults.json"
    with mlb_defaults_path.open("r") as handle:
        mlb_defaults = json.load(handle)

    weighted_fwar, weighted_meta = compute_weighted_fwar(
        name_key,
        fwar_weight_seasons,
        fwar_weights,
        war_history,
        mlb_id=player.get("mlb_id"),
    )
    is_two_way = name_key in two_way_names
    player_age = player.get("age")
    weighted_fwar, weighted_meta = compute_weighted_fwar(
        name_key,
        fwar_weight_seasons,
        fwar_weights,
        war_history,
        mlb_id=player.get("mlb_id"),
    )
    if weighted_fwar is None:
        fwar_source = "snapshot_fwar"
        fwar_pre_regress = base_fwar
    else:
        fwar_source = "weighted_history"
        fwar_pre_regress = weighted_fwar

    is_pitcher = name_key in pitcher_names and not is_two_way
    pitcher_regress_applied = False
    if is_pitcher and pitcher_regress_weight > 0:
        fwar_post_regress = (
            1.0 - pitcher_regress_weight
        ) * fwar_pre_regress + pitcher_regress_weight * pitcher_regress_target
        pitcher_regress_applied = fwar_post_regress != fwar_pre_regress
    else:
        fwar_post_regress = fwar_pre_regress

    reliever_mult_applied = reliever_mult if is_reliever else 1.0
    two_way_mult_applied = two_way_mult if is_two_way else 1.0
    base_fwar = fwar_post_regress * reliever_mult_applied * two_way_mult_applied

    mlb_defaults_path = repo_root / "tvp_mlb_defaults.json"
    with mlb_defaults_path.open("r") as handle:
        mlb_defaults = json.load(handle)

    regressed_fwar, mean_reversion_meta = apply_mean_reversion(
        base_fwar=base_fwar,
        is_hitter=not is_pitcher,
        player_age=player_age,
        mlb_defaults=mlb_defaults,
    )

    fwar_for_projection = regressed_fwar

    fwar_cap_to_use = fwar_cap
    if (
        is_two_way
        and two_way_fwar_cap is not None
        and (fwar_cap is None or two_way_fwar_cap > fwar_cap)
    ):
        fwar_cap_to_use = two_way_fwar_cap
    fwar_scale_to_use = fwar_scale
    young_player_applied = False
    if (
        player_age is not None
        and player_age <= young_player_max_age
        and young_player_scale > 0
    ):
        fwar_scale_to_use = young_player_scale
        young_player_applied = True

    contract = player.get("contract")
    if not contract:
        return {
            "mlb_id": player.get("mlb_id"),
            "player_name": player.get("player_name"),
            "age": player.get("age"),
            "status": "mlb",
            "tvp_prospect": None,
            "tvp_mlb": None,
            "tvp_current": None,
            "raw_components": {"error": "missing_contract"},
            "snapshot_year": snapshot_year,
            "last_updated_timestamp": now_timestamp(),
        }

    contract_years = contract.get("contract_years") or []
    salary_by_season, missing_salary_seasons = build_salary_map(
        contract_years, snapshot_year
    )

    option_years_raw = contract.get("options") or []
    contract_years_remaining = contract.get("years_remaining")
    aav_m = contract.get("aav_m")
    fallback_contract = None
    fallback_used = False
    if isinstance(contract_years_remaining, int) and contract_years_remaining > 0:
        horizon = min(contract_years_remaining, max_years)
        all_seasons = list(range(snapshot_year, snapshot_year + horizon))
    else:
        all_seasons = sorted(
            set(salary_by_season.keys())
            | {
                opt.get("season")
                for opt in option_years_raw
                if isinstance(opt.get("season"), int)
            }
        )
        all_seasons = [season for season in all_seasons if season >= snapshot_year]
        if max_years and all_seasons:
            all_seasons = [
                season for season in all_seasons if season < snapshot_year + max_years
            ]

    control_years_seeded = False
    if not all_seasons:
        fallback_contract = contracts_2026_map.get(name_key)
        if fallback_contract:
            all_seasons = fallback_contract.get("seasons", [])
            salary_by_season = fallback_contract.get("salary_by_season", {}).copy()
            missing_salary_seasons = set()
            fallback_used = True
    if not all_seasons:
        player_age = player.get("age")
        if (
            player_age is not None
            and player_age <= control_years_age_max
            and control_years_fallback > 0
        ):
            all_seasons = list(
                range(snapshot_year, snapshot_year + control_years_fallback)
            )
            control_years_seeded = True
        else:
            return {
                "mlb_id": player.get("mlb_id"),
                "player_name": player.get("player_name"),
                "age": player.get("age"),
                "status": "mlb",
                "tvp_prospect": None,
                "tvp_mlb": None,
                "tvp_current": None,
                "raw_components": {
                    "error": "missing_contract_years",
                    "quality_flags": {
                        "positions_missing": positions_missing,
                    },
                },
                "snapshot_year": snapshot_year,
                "last_updated_timestamp": now_timestamp(),
            }

    last_season = max(all_seasons)
    seasons = list(range(snapshot_year, last_season + 1))
    control_fallback_seasons: set[int] = set(seasons) if control_years_seeded else set()

    if not fallback_used:
        existing_seasons = set(seasons)
        seasons, salary_by_season, missing_salary_seasons, control_years_extended = (
            apply_control_year_fallback(
                seasons,
                salary_by_season,
                missing_salary_seasons,
                snapshot_year,
                player.get("age"),
                control_years_fallback,
                control_years_age_max,
                config,
            )
        )
        if control_years_extended:
            if control_years_seeded:
                control_fallback_seasons = set(seasons)
            else:
                control_fallback_seasons = set(seasons) - existing_seasons
    else:
        control_years_extended = False
    control_years_applied = control_years_seeded or control_years_extended
    projected_fwar, age_by_season, aging_mults = project_fwar_flat(
        fwar_for_projection,
        seasons,
        fwar_scale_to_use,
        fwar_cap_to_use,
        player_age,
        snapshot_year,
        apply_aging,
        prime_age,
        decline_per_year,
        aging_floor,
        player.get("player_name"),
        position,
    )

    # Apply catcher-specific risk adjustments
    catcher_risk_meta: dict[str, Any] = {"applied": False, "adjustments": {}}
    fwar_before_catcher_risk: dict[int, float] = {}
    if is_catcher and not disable_catcher_adjust:
        fwar_before_catcher_risk = dict(projected_fwar)
        projected_fwar, catcher_risk_meta = apply_catcher_risk_adjustments(
            projected_fwar,
            age_by_season,
            snapshot_year,
            player_age,
            is_catcher,
            mlb_defaults,
            player.get("player_name"),
            aging_mults,
            apply_aging,
            prime_age,
            decline_per_year,
            aging_floor,
        )
        catcher_risk_meta["applied"] = True
    else:
        catcher_risk_meta["applied"] = False

    option_years, option_seasons = build_option_years(
        option_years_raw, snapshot_year, projected_fwar
    )

    salary_source = "contract_years"
    if fallback_used:
        salary_source = "contracts_2026_aav"
    has_contract_values = any(value is not None for value in salary_by_season.values())
    if (
        not has_contract_values
        and aav_m is not None
        and isinstance(aav_m, (int, float))
        and seasons
    ):
        salary_source = "aav_m"
        salary_by_season = {season: float(aav_m) for season in seasons}
        missing_salary_seasons = set()

    player_name = player.get("player_name")
    override_salary = (
        SPECIAL_SALARY_OVERRIDES_M.get(player_name) if player_name else None
    )
    if override_salary is not None and seasons:
        # Treat deferrals as tax AAV for TVP valuation.
        salary_source = "manual_override_aav"
        salary_by_season = {season: float(override_salary) for season in seasons}
        missing_salary_seasons = set()

    control_salary_floor_seasons: set[int] = set()

    control_fallback_data = {}
    if control_fallback_seasons:
        salary_components_by_t = {}
        for season in sorted(control_fallback_seasons):
            t = season - snapshot_year
            min_salary = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
            current_salary = salary_by_season.get(season)
            if current_salary is None:
                clamped_salary = min_salary
                control_salary_floor_seasons.add(season)
            else:
                clamped_salary = max(current_salary, min_salary)
                if clamped_salary != current_salary:
                    control_salary_floor_seasons.add(season)
            salary_components_by_t[str(t)] = {
                "min_salary_t": min_salary,
                "initial_salary_t": current_salary,
                "final_salary_t": clamped_salary,
            }
            salary_by_season[season] = clamped_salary
        control_fallback_data = {"salary_components_by_t": salary_components_by_t}

    salary_fallback_seasons: set[int] = set(control_salary_floor_seasons)
    fwar_by_year_base: list[float] = []
    salary_by_year_base: list[float] = []
    for season in seasons:
        if season in option_seasons:
            fwar_by_year_base.append(0.0)
            salary_by_year_base.append(0.0)
            continue
        fwar_by_year_base.append(projected_fwar.get(season, 0.0))
        salary_value = salary_by_season.get(season)
        if salary_value is None:
            salary_value = config.min_salary_m
            if season in salary_by_season:
                salary_fallback_seasons.add(season)
        salary_by_year_base.append(float(salary_value))

    mlb_result = compute_mlb_tvp(
        fwar_by_year_base,
        salary_by_year_base,
        config,
        option_years=option_years,
    )
    eligible_salary = [
        salary_by_year_base[idx]
        for idx, season in enumerate(seasons)
        if season not in option_seasons
    ]
    years_remaining = len(eligible_salary)
    avg_salary_m = (
        sum(eligible_salary) / years_remaining if years_remaining > 0 else None
    )
    long_control_boost_pct = 0.0
    long_control_boost_value = 0.0
    if (
        mlb_result["tvp_mlb"] > 0
        and years_remaining > LONG_CONTROL_BASELINE_YEARS
        and avg_salary_m is not None
        and avg_salary_m <= LONG_CONTROL_AAV_MAX
    ):
        years_over_baseline = years_remaining - LONG_CONTROL_BASELINE_YEARS
        long_control_boost_pct = min(
            LONG_CONTROL_MAX_BOOST_PCT,
            LONG_CONTROL_STEP_BOOST_PCT * years_over_baseline,
        )
        long_control_boost_value = mlb_result["tvp_mlb"] * long_control_boost_pct
        mlb_result["tvp_mlb"] += long_control_boost_value

    # Apply rookie transition blend when prospect anchors and early MLB samples are available.
    tvp_mlb_value = mlb_result["tvp_mlb"]
    tvp_mlb_base_value = mlb_result.get("tvp_mlb_base", tvp_mlb_value)
    tvp_current = tvp_mlb_value
    rookie_transition: dict[str, Any] = {
        "applied": False,
        "tvp_current_pre": tvp_mlb_value,
        "tvp_current_post": tvp_mlb_value,
        "delta": 0.0,
    }
    prospect_anchor = player.get("prospect_anchor")
    fv_value = None
    prospect_tvp = None
    sample_pa = player.get("pa")
    sample_ip = player.get("ip")
    sample_bat_war = player.get("bat_war")
    sample_pit_war = player.get("pit_war")
    pa_value = float(sample_pa) if isinstance(sample_pa, (int, float)) else None
    ip_value = float(sample_ip) if isinstance(sample_ip, (int, float)) else None
    if is_pitcher:
        fwar_to_date = (
            float(sample_pit_war)
            if isinstance(sample_pit_war, (int, float))
            else float(player.get("fwar") or 0.0)
        )
    else:
        fwar_to_date = (
            float(sample_bat_war)
            if isinstance(sample_bat_war, (int, float))
            else float(player.get("fwar") or 0.0)
        )
    seasons_used = weighted_meta.get("seasons_count", 0)
    used_sample_gate = pa_value is not None or ip_value is not None
    early_sample_eligible = False
    if player_age is not None and player_age <= 26:
        if is_pitcher:
            early_sample_eligible = ip_value is not None and ip_value < 80
        else:
            early_sample_eligible = pa_value is not None and pa_value < 300

    anchor_source = None
    anchor_tvp = None
    if isinstance(prospect_anchor, dict):
        prospect_tvp = prospect_anchor.get("tvp_prospect")
        fv_value = prospect_anchor.get("fv_value")
        if fv_value is None:
            fv_value = (prospect_anchor.get("raw_components") or {}).get("fv_value")
        anchor_source = "prospect"
    elif early_sample_eligible:
        fallback_fv_value = (
            min(config.fv_war_rate_prior) if config.fv_war_rate_prior else 50
        )
        prospect_tvp = float(tvp_mlb_base_value)
        fv_value = fallback_fv_value
        anchor_source = "mlb_baseline_fallback"
    if isinstance(prospect_tvp, (int, float)):
        anchor_tvp = float(prospect_tvp)

    applied_flag = False
    alpha_info = None
    reason_not_applied = None
    if (
        anchor_source == "prospect"
        and early_sample_eligible
        and isinstance(fv_value, (int, float))
        and isinstance(prospect_tvp, (int, float))
    ):
        alpha_info = compute_rookie_alpha(
            int(fv_value),
            is_pitcher,
            pa_value,
            ip_value,
            fwar_to_date,
            config,
        )
        alpha = alpha_info["alpha"]
        tvp_current = alpha * float(prospect_tvp) + (1.0 - alpha) * tvp_mlb_value
        applied_flag = True
    elif anchor_source == "mlb_baseline_fallback":
        reason_not_applied = "fallback_anchor_noop"
        if early_sample_eligible and isinstance(fv_value, (int, float)):
            alpha_info = compute_rookie_alpha(
                int(fv_value),
                is_pitcher,
                pa_value,
                ip_value,
                fwar_to_date,
                config,
            )
    elif anchor_source == "prospect":
        reason_not_applied = "not_early_sample"
    else:
        reason_not_applied = "missing_anchor"

    delta_value = tvp_current - tvp_mlb_value
    noop_warning = False

    # Enforce catcher haircut caps
    haircut_raw_pct = None
    haircut_cap_value = None
    haircut_capped_pct = None
    haircut_cap_applied = False

    if (
        is_catcher
        and catcher_risk_meta.get("applied", False)
        and fwar_before_catcher_risk
    ):
        # Calculate baseline TVP from pre-catcher-risk WAR
        # Reconstruct fwar_by_year_base without catcher adjustments
        fwar_by_year_baseline = [
            fwar_before_catcher_risk.get(season, projected_fwar.get(season, 0.0))
            for season in seasons
        ]

        # Recompute baseline TVP
        mlb_result_baseline = compute_mlb_tvp(
            fwar_by_year_baseline,
            salary_by_year_base,
            config,
            option_years=option_years,
        )
        baseline_tvp = mlb_result_baseline.get("tvp_mlb", tvp_mlb_value)

        # Calculate raw haircut percentage
        if baseline_tvp > 0:
            haircut_raw_pct = (1.0 - tvp_mlb_value / baseline_tvp) * 100.0
        else:
            haircut_raw_pct = 0.0

        # Determine age-based cap
        if player_age is not None:
            if player_age <= 26:
                haircut_cap_pct = 25.0
            elif 27 <= player_age <= 29:
                haircut_cap_pct = 35.0
            else:
                haircut_cap_pct = None
        else:
            haircut_cap_pct = None

        # Apply cap if needed
        if haircut_cap_pct is not None and haircut_raw_pct is not None:
            if haircut_raw_pct > haircut_cap_pct:
                # Haircut exceeds cap - scale TVP up to capped level
                min_tvp_pct = (100.0 - haircut_cap_pct) / 100.0
                capped_tvp_mlb_value = baseline_tvp * min_tvp_pct
                capped_tvp_current = (
                    tvp_current * (capped_tvp_mlb_value / tvp_mlb_value)
                    if tvp_mlb_value > 0
                    else capped_tvp_mlb_value
                )
                haircut_capped_pct = haircut_cap_pct
                haircut_cap_applied = True

                # Update TVP values
                tvp_mlb_value = capped_tvp_mlb_value
                tvp_current = capped_tvp_current
            else:
                haircut_capped_pct = haircut_raw_pct
                haircut_cap_applied = False
        else:
            haircut_capped_pct = haircut_raw_pct
            haircut_cap_applied = False

    if (
        anchor_source is None
        or anchor_source == "mlb_baseline_fallback"
        or anchor_source == "not_early_sample"
        or anchor_source == "missing_anchor"
    ):
        noop_warning = True

    rookie_transition.update(
        {
            "applied": applied_flag,
            "reason_not_applied": reason_not_applied,
            "pa": pa_value,
            "ip": ip_value,
            "fwar_to_date": fwar_to_date,
            "fv_value": fv_value,
            "prospect_tvp": float(prospect_tvp)
            if isinstance(prospect_tvp, (int, float))
            else prospect_tvp,
            "anchor_tvp": anchor_tvp,
            "anchor_source": anchor_source,
            "tvp_current_pre": tvp_mlb_value,
            "tvp_current_post": tvp_current,
            "delta": delta_value,
            "source_file": prospect_anchor.get("source_file")
            if isinstance(prospect_anchor, dict)
            else None,
            "early_sample_eligible": early_sample_eligible,
            "gate": {
                "used_pa_ip": used_sample_gate,
                "age": player_age,
                "war_history_seasons_used": seasons_used,
            },
            "noop_anchor_delta_mismatch": noop_warning,
        }
    )
    if alpha_info is not None:
        rookie_transition.update(
            {
                "alpha": alpha_info.get("alpha"),
                "alpha_base": alpha_info.get("alpha_base"),
                "evidence": alpha_info.get("evidence"),
            }
        )

    mlb_raw = mlb_result["raw_components"]
    guaranteed_seasons = [season for season in seasons if season not in option_seasons]
    t_to_year = {str(t): season for t, season in enumerate(seasons)}
    guaranteed_fwar_by_t = {
        str(t): fwar
        for t, fwar in enumerate(fwar_by_year_base)
        if seasons[t] not in option_seasons
    }
    option_fwar_by_t = {
        str(season - snapshot_year): projected_fwar.get(season, 0.0)
        for season in option_seasons
    }
    aging_mult_by_t = {
        str(t): aging_mults.get(season, 1.0) for t, season in enumerate(seasons)
    }
    price_by_t = {
        str(t): price for t, price in enumerate(mlb_raw.get("price_by_year", []))
    }
    value_by_t = {
        str(t): value for t, value in enumerate(mlb_raw.get("value_by_year", []))
    }
    salary_by_t = {
        str(t): value for t, value in enumerate(mlb_raw.get("salary_by_year", []))
    }
    surplus_by_t = {
        str(t): value for t, value in enumerate(mlb_raw.get("surplus_by_year", []))
    }
    discount_by_t = {
        str(t): value for t, value in enumerate(mlb_raw.get("discount_factors", []))
    }
    pv_surplus_by_t = {
        str(t): value for t, value in enumerate(mlb_raw.get("pv_surplus_by_year", []))
    }

    options_detail = mlb_raw.get("options", [])
    options_pv_total = sum((opt.get("pv_ev") or 0.0) for opt in options_detail)
    options_audit = [
        {
            "t": opt.get("t"),
            "type": opt.get("option_type"),
            "fwar_used_for_option": opt.get("fwar_used_for_option"),
            "S": opt.get("S"),
            "B": opt.get("B"),
            "V": opt.get("V"),
            "M": opt.get("market"),
            "sigmoid_input_ex": opt.get("sigmoid_input_exercise"),
            "sigmoid_input_in": opt.get("sigmoid_input_in"),
            "P_ex": (opt.get("probabilities") or {}).get("P_ex"),
            "P_in": (opt.get("probabilities") or {}).get("P_in"),
            "P_team": (opt.get("probabilities") or {}).get("P_team"),
            "P_player": (opt.get("probabilities") or {}).get("P_player"),
            "probabilities": opt.get("probabilities"),
            "EV": opt.get("ev"),
            "pv_EV": opt.get("pv_ev"),
        }
        for opt in options_detail
    ]
    if control_years_applied:
        contract_source = "control_fallback"
    elif salary_source == "contracts_2026_aav":
        contract_source = "sqlite_aav"
    else:
        contract_source = "contract_years"
    seasons_count = weighted_meta.get("seasons_count", 0)
    if fwar_source == "weighted_history":
        if seasons_count >= len(fwar_weights):
            fwar_source_label = "history_weighted_full"
        else:
            fwar_source_label = "history_weighted_partial"
    else:
        fwar_source_label = "snapshot"

    return {
        "mlb_id": player.get("mlb_id"),
        "player_name": player.get("player_name"),
        "age": player.get("age"),
        "status": "mlb",
        "tvp_prospect": None,
        "tvp_mlb": tvp_mlb_value,
        "tvp_current": tvp_current,
        "raw_components": {
            "projection": {
                "method": "flat_from_latest_fwar_scaled",
                "base_fwar": base_fwar,
                "fwar_source": fwar_source,
                "fwar_source_label": fwar_source_label,
                "fwar_weight_seasons": fwar_weight_seasons,
                "fwar_weights": fwar_weights,
                "weighted_fwar": weighted_fwar,
                "weighted_fwar_meta": weighted_meta,
                "is_pitcher": is_pitcher,
                "is_reliever": is_reliever,
                "is_two_way": is_two_way,
                "is_catcher": is_catcher,
                "position": position,
                "position_source": position_source,
                "pitcher_regress_weight": pitcher_regress_weight,
                "pitcher_regress_target": pitcher_regress_target,
                "pitcher_regress_applied": is_pitcher and pitcher_regress_weight > 0,
                "pitcher_qualified": is_pitcher,
                "fwar_pre_regress": fwar_pre_regress,
                "fwar_post_regress": fwar_post_regress,
                "reliever_mult": reliever_mult,
                "reliever_mult_applied": reliever_mult_applied,
                "two_way_qualified": is_two_way,
                "two_way_mult": two_way_mult,
                "two_way_mult_applied": two_way_mult_applied,
                "catcher_risk_applied": catcher_risk_meta.get(
                    "catcher_risk_applied", False
                ),
                "catcher_risk_config": catcher_risk_meta.get("config"),
                "catcher_risk_adjustments": catcher_risk_meta.get("adjustments"),
                "fwar_before_catcher_risk": fwar_before_catcher_risk
                if fwar_before_catcher_risk
                else None,
                "haircut_raw_pct": haircut_raw_pct,
                "haircut_capped_pct": haircut_capped_pct,
                "haircut_cap_value_pct": haircut_cap_value,
                "haircut_cap_applied": haircut_cap_applied,
                "fwar_scale": fwar_scale,
                "fwar_cap": fwar_cap,
                "fwar_cap_applied": fwar_cap_to_use,
                "fwar_scale": fwar_scale,
                "fwar_scale_applied": fwar_scale_to_use,
                "fwar_scale_used": fwar_scale_to_use,
                "fwar_cap_used": fwar_cap_to_use,
                "young_player_max_age": young_player_max_age,
                "young_player_scale": young_player_scale,
                "young_player_applied": young_player_applied,
                "apply_aging": apply_aging,
                "prime_age": prime_age,
                "decline_per_year": decline_per_year,
                "aging_floor": aging_floor,
                "age_by_season": age_by_season,
                "aging_mult_by_season": aging_mults,
                "projected_fwar_by_season": projected_fwar,
                "t_to_year": t_to_year,
                "guaranteed_fwar_by_t": guaranteed_fwar_by_t,
                "option_fwar_by_t": option_fwar_by_t,
                "aging_mult_by_t": aging_mult_by_t,
                "mean_reversion": mean_reversion_meta,
                "base_fwar": base_fwar,
                "fwar_used_for_projection": fwar_for_projection,
            },
            "contract": {
                "seasons": seasons,
                "salary_source": salary_source,
                "contract_source": contract_source,
                "salary_by_season": salary_by_season,
                "salary_missing_seasons": sorted(missing_salary_seasons),
                "salary_fallback_seasons": sorted(salary_fallback_seasons),
                "control_salary_floor_seasons": sorted(control_salary_floor_seasons),
                "option_seasons": sorted(option_seasons),
                "control_years_fallback_applied": control_years_applied,
                "contracts_2026_fallback_used": fallback_used,
                "contracts_2026_contract": fallback_contract,
                "control_fallback": control_fallback_data,
            },
            "mlb": mlb_result["raw_components"],
            "long_control_low_aav": {
                "applied": long_control_boost_pct > 0,
                "years_remaining": years_remaining,
                "years_over_baseline": years_remaining - LONG_CONTROL_BASELINE_YEARS,
                "baseline_years": LONG_CONTROL_BASELINE_YEARS,
                "avg_salary_m": avg_salary_m,
                "boost_pct": long_control_boost_pct,
                "boost_value": long_control_boost_value,
            },
            "war_inputs": {
                "fwar_source_label": fwar_source_label,
                "fwar_source_raw": fwar_source,
                "war_history_used": weighted_meta.get("seasons"),
                "war_history_seasons_used": weighted_meta.get("seasons_count", 0)
                if fwar_source == "weighted_history"
                else 0,
                "weights_sum": weighted_meta.get("weights_sum", 0.0)
                if fwar_source == "weighted_history"
                else 0.0,
                "weighted_fwar": weighted_fwar,
                "is_pitcher": is_pitcher,
                "pitcher_qualified": is_pitcher,
                "pitcher_regress_applied": pitcher_regress_applied,
                "is_reliever": is_reliever,
                "is_two_way": is_two_way,
                "pitcher_regress_weight": pitcher_regress_weight,
                "pitcher_regress_target": pitcher_regress_target,
                "fwar_pre_regress": fwar_pre_regress,
                "fwar_post_regress": fwar_post_regress,
                "snapshot_fwar_used": player.get("fwar")
                if weighted_fwar is None
                else None,
                "reliever_mult_applied": reliever_mult_applied,
                "two_way_mult_applied": two_way_mult_applied,
                "cap_used": fwar_cap_to_use,
            },
            "projection_scaling_aging": {
                "fwar_scale_used": fwar_scale_to_use,
                "fwar_cap_used": fwar_cap_to_use,
                "aging_mult_by_t": aging_mult_by_t,
                "guaranteed_fwar_by_t": guaranteed_fwar_by_t,
                "option_fwar_by_t": option_fwar_by_t,
                "years_projected": len(fwar_by_year_base),
                "t_to_year": t_to_year,
            },
            "contract_salaries": {
                "contract_source": contract_source,
                "salary_source_raw": salary_source,
                "salary_by_year_m": salary_by_t,
                "missing_salary_filled_min_count": len(salary_fallback_seasons),
                "avg_salary_m": avg_salary_m,
                "years_remaining": years_remaining,
            },
            "economics_pv": {
                "price_by_year_m": price_by_t,
                "value_by_year_m": value_by_t,
                "surplus_by_year_m": surplus_by_t,
                "discount_by_year": discount_by_t,
                "pv_surplus_by_year_m": pv_surplus_by_t,
                "tvp_mlb_base": mlb_result["tvp_mlb_base"],
                "options_pv_total": options_pv_total,
            },
            "options": options_audit,
            "rookie_transition": rookie_transition,
            "long_control_boost": {
                "boost_applied": long_control_boost_pct > 0,
                "boost_pct": long_control_boost_pct,
                "boost_amount": long_control_boost_value,
            },
            "quality_flags": {
                "positions_missing": positions_missing,
                "war_history_partial": fwar_source_label == "history_weighted_partial",
                "war_history_seasons_used": weighted_meta.get("seasons_count", 0),
                "war_history_weights_sum": weighted_meta.get("weights_sum")
                if fwar_source == "weighted_history"
                else None,
                "projected_war_hits_cap": any(
                    math.isclose(projected_fwar.get(s, 0.0), fwar_cap_to_use)
                    if fwar_cap_to_use is not None
                    else False
                    for s in seasons
                ),
                "projected_war_years": len(seasons),
                "salary_below_min_detected": len(control_salary_floor_seasons) > 0,
                "control_fallback_used": contract_source == "control_fallback",
                "option_years_detected": sorted(option_seasons),
                "options_present": len(option_years_raw) > 0,
                "option_year_mismatch": (len(option_years_raw) > 0)
                != (len(option_seasons) > 0),
                "salary_below_min_detected": (
                    len(control_salary_floor_seasons) > 0
                    or any(
                        (salary := salary_by_season.get(season)) is not None
                        and salary
                        < config.min_salary_m
                        * ((1.0 + config.min_salary_growth) ** (season - snapshot_year))
                        for season in seasons
                    )
                ),
                "guaranteed_includes_option_year": any(
                    season in option_seasons
                    and (salary := salary_by_season.get(season)) is not None
                    and salary > 0.0
                    for season in seasons
                ),
            },
            "fallbacks": {
                "control_years_fallback_applied": control_years_applied,
                "contracts_2026_fallback_used": fallback_used,
                "contracts_2026_contract": fallback_contract,
            },
        },
        "snapshot_year": snapshot_year,
        "last_updated_timestamp": now_timestamp(),
    }


def now_timestamp() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def parse_weights(raw: str) -> list[float]:
    parts = [item.strip() for item in raw.split(",") if item.strip()]
    if not parts:
        return []
    return [float(item) for item in parts]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MLB TVP outputs.")
    parser.add_argument("--config", type=Path, help="Path to tvp_config.json")
    parser.add_argument(
        "--players", type=Path, help="Path to players_with_contracts_*.json"
    )
    parser.add_argument("--output", type=Path, help="Output JSON path")
    parser.add_argument(
        "--max-years",
        type=int,
        default=10,
        help="Maximum projection years from snapshot year.",
    )
    parser.add_argument(
        "--fwar-scale",
        type=float,
        default=0.70,
        help="Scale applied to base fWAR before projecting.",
    )
    parser.add_argument(
        "--fwar-cap",
        type=float,
        default=6.0,
        help="Maximum projected fWAR per season (set <=0 to disable).",
    )
    parser.add_argument(
        "--fwar-weights",
        type=str,
        default="0.5,0.3,0.2",
        help="Comma-separated weights for recent fWAR seasons (most recent first).",
    )
    parser.add_argument(
        "--pitcher-regress-weight",
        type=float,
        default=0.35,
        help="Weight to regress pitcher fWAR toward the target.",
    )
    parser.add_argument(
        "--pitcher-regress-target",
        type=float,
        default=2.0,
        help="Target fWAR for pitcher regression.",
    )
    parser.add_argument(
        "--young-player-max-age",
        type=int,
        default=24,
        help="Max age for young-player fWAR scale boost.",
    )
    parser.add_argument(
        "--young-player-scale",
        type=float,
        default=1.0,
        help="fWAR scale applied to young players (overrides base fwar_scale).",
    )
    parser.add_argument(
        "--no-aging-curve",
        action="store_true",
        help="Disable age-based decline.",
    )
    parser.add_argument(
        "--disable-catcher-adjust",
        action="store_true",
        help="Disable catcher-specific risk adjustments (for testing).",
    )
    parser.add_argument(
        "--prime-age",
        type=int,
        default=29,
        help="Age at which decline begins.",
    )
    parser.add_argument(
        "--decline-per-year",
        type=float,
        default=0.035,
        help="Linear decline per year after prime age.",
    )
    parser.add_argument(
        "--aging-floor",
        type=float,
        default=0.65,
        help="Minimum aging multiplier.",
    )
    parser.add_argument(
        "--reliever-mult",
        type=float,
        default=1.5,
        help="Multiplier applied to reliever WAR.",
    )
    parser.add_argument(
        "--two-way-fwar-cap",
        type=float,
        default=8.0,
        help="Higher fWAR cap for qualified two-way players (set <=0 to disable).",
    )
    parser.add_argument(
        "--two-way-min-war",
        type=float,
        default=1.0,
        help="Minimum batting and pitching WAR to qualify as two-way.",
    )
    parser.add_argument(
        "--two-way-mult",
        type=float,
        default=1.5,
        help="Multiplier applied to two-way player WAR.",
    )
    parser.add_argument(
        "--control-years-fallback",
        type=int,
        default=4,
        help="Assumed control years for young players missing contract years.",
    )
    parser.add_argument(
        "--control-years-age-max",
        type=int,
        default=27,
        help="Max age to apply control-years fallback.",
    )
    parser.add_argument(
        "--no-position-refresh",
        action="store_true",
        help="Do not auto-build positions map if missing.",
    )
    parser.add_argument(
        "--allow-missing-positions",
        action="store_true",
        help="Allow run to proceed when positions map is missing.",
    )
    args = parser.parse_args()

    disable_catcher_adjust = (
        args.disable_catcher_adjust
        if hasattr(args, "disable_catcher_adjust")
        else False
    )

    repo_root = Path(__file__).resolve().parents[1]
    config_path = args.config or (repo_root / "backend" / "tvp_config.json")
    players_path = args.players or (
        repo_root / "backend" / "output" / "players_with_contracts_2025.json"
    )

    payload = load_players(players_path)
    snapshot_year = load_config(config_path).snapshot_year
    snapshot_season = payload.get("meta", {}).get("season", snapshot_year)
    age_offset = 0
    if isinstance(snapshot_season, int):
        age_offset = max(0, snapshot_year - snapshot_season)
    stats_db_path = Path(__file__).with_name("stats.db")
    contracts_2026_path = repo_root / "mlb_2026_contracts_all_teams.sqlite"
    reliever_names = load_reliever_names(snapshot_season, stats_db_path)
    pitcher_names = load_pitcher_names(snapshot_season, stats_db_path)
    two_way_names = load_two_way_names(
        snapshot_season,
        stats_db_path,
        args.two_way_min_war,
    )

    positions_path = repo_root / "backend" / "data" / "player_positions.json"
    positions_map, positions_missing = ensure_positions_map(
        positions_path,
        players_path,
        allow_missing=args.allow_missing_positions,
        no_position_refresh=args.no_position_refresh,
    )
    position_by_id = attach_positions(payload.get("players", []), positions_map)
    catcher_ids = build_catcher_ids(position_by_id)
    fwar_weights = parse_weights(args.fwar_weights)
    if not fwar_weights:
        fwar_weights = [1.0]
    fwar_weight_seasons = [
        snapshot_season - offset for offset in range(len(fwar_weights))
    ]
    war_history = load_war_history(fwar_weight_seasons, stats_db_path)
    sample_counts = load_sample_counts(snapshot_season, stats_db_path)
    prospects_by_id, prospects_by_name = load_prospect_anchors(repo_root)
    contracts_2026_map = load_contracts_2026_map(contracts_2026_path, snapshot_year)
    enrich_players(
        payload.get("players", []),
        sample_counts,
        prospects_by_id,
        prospects_by_name,
    )
    results = [
        compute_player_tvp(
            adjust_player_age(player, age_offset),
            snapshot_year,
            config_path,
            args.max_years,
            args.fwar_scale,
            None if args.fwar_cap <= 0 else args.fwar_cap,
            not args.no_aging_curve,
            args.prime_age,
            args.decline_per_year,
            args.aging_floor,
            reliever_names,
            args.reliever_mult,
            args.control_years_fallback,
            args.control_years_age_max,
            two_way_names,
            None if args.two_way_fwar_cap <= 0 else args.two_way_fwar_cap,
            args.two_way_mult,
            war_history,
            fwar_weights,
            fwar_weight_seasons,
            pitcher_names,
            args.pitcher_regress_weight,
            args.pitcher_regress_target,
            contracts_2026_map,
            args.young_player_max_age,
            args.young_player_scale,
            catcher_ids,
            positions_missing,
            disable_catcher_adjust,
        )
        for player in payload.get("players", [])
    ]

    generated_at = now_timestamp()
    output_path = args.output or (
        repo_root / "backend" / "output" / f"tvp_mlb_{snapshot_year}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "meta": {
            "generated_at": generated_at,
            "snapshot_year": snapshot_year,
            "source_file": str(players_path),
            "source_season": payload.get("meta", {}).get("season"),
            "age_offset": age_offset,
            "player_count": len(results),
            "max_years": args.max_years,
            "fwar_scale": args.fwar_scale,
            "fwar_cap": None if args.fwar_cap <= 0 else args.fwar_cap,
            "fwar_weights": fwar_weights,
            "fwar_weight_seasons": fwar_weight_seasons,
            "pitcher_regress_weight": args.pitcher_regress_weight,
            "pitcher_regress_target": args.pitcher_regress_target,
            "young_player_max_age": args.young_player_max_age,
            "young_player_scale": args.young_player_scale,
            "long_control_baseline_years": LONG_CONTROL_BASELINE_YEARS,
            "long_control_aav_max": LONG_CONTROL_AAV_MAX,
            "long_control_step_boost_pct": LONG_CONTROL_STEP_BOOST_PCT,
            "long_control_max_boost_pct": LONG_CONTROL_MAX_BOOST_PCT,
            "aging_curve": not args.no_aging_curve,
            "prime_age": args.prime_age,
            "decline_per_year": args.decline_per_year,
            "aging_floor": args.aging_floor,
            "reliever_mult": args.reliever_mult,
            "reliever_count": len(reliever_names),
            "pitcher_count": len(pitcher_names),
            "catcher_count": len(catcher_ids),
            "catcher_war_mult": (
                repo_root / "backend" / "tvp_mlb_defaults.json"
            ).exists()
            and json.loads(
                (repo_root / "backend" / "tvp_mlb_defaults.json").read_text()
            ).get("catcher_war_mult", 1.0)
            or 1.0,
            "two_way_fwar_cap": None
            if args.two_way_fwar_cap <= 0
            else args.two_way_fwar_cap,
            "two_way_min_war": args.two_way_min_war,
            "two_way_count": len(two_way_names),
            "two_way_mult": args.two_way_mult,
            "control_years_fallback": args.control_years_fallback,
            "control_years_age_max": args.control_years_age_max,
            "contracts_2026_fallback_count": sum(
                1
                for player in results
                if player.get("raw_components", {})
                .get("contract", {})
                .get("contracts_2026_fallback_used")
            ),
        },
        "players": results,
    }

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=True)

    print(f"Wrote {len(results)} MLB players to {output_path}")


if __name__ == "__main__":
    main()
