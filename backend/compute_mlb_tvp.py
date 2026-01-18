from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sqlite3
import re

from tvp_engine import compute_mlb_tvp, load_config


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
    return re.sub(r"[^a-z]", "", (name or "").lower())


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
) -> dict[str, dict[int, dict[str, float]]]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    placeholders = ",".join("?" for _ in seasons)
    params = tuple(seasons)
    batting: dict[str, dict[int, float]] = {}
    pitching: dict[str, dict[int, float]] = {}

    cursor.execute(
        f"""
        SELECT name, season, war
        FROM batting_stats
        WHERE season IN ({placeholders})
        """,
        params,
    )
    for name, season, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        batting.setdefault(key, {})
        batting[key][season] = batting[key].get(season, 0.0) + float(war)

    cursor.execute(
        f"""
        SELECT name, season, war
        FROM pitching_stats
        WHERE season IN ({placeholders})
        """,
        params,
    )
    for name, season, war in cursor.fetchall():
        if name is None or war is None:
            continue
        key = normalize_name(name)
        pitching.setdefault(key, {})
        pitching[key][season] = pitching[key].get(season, 0.0) + float(war)

    conn.close()

    history: dict[str, dict[int, dict[str, float]]] = {}
    for key in set(batting) | set(pitching):
        history[key] = {}
        for season in seasons:
            bat_seasons = batting.get(key, {})
            pit_seasons = pitching.get(key, {})
            if season not in bat_seasons and season not in pit_seasons:
                continue
            bat = bat_seasons.get(season, 0.0)
            pit = pit_seasons.get(season, 0.0)
            history[key][season] = {"bat": bat, "pit": pit}
    return history


def compute_weighted_fwar(
    name_key: str,
    seasons: list[int],
    weights: list[float],
    war_history: dict[str, dict[int, dict[str, float]]],
) -> tuple[float | None, dict[str, Any]]:
    if name_key not in war_history:
        return None, {"source": "missing_history", "seasons": seasons}
    season_war = []
    for season, weight in zip(seasons, weights):
        entry = war_history.get(name_key, {}).get(season)
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
) -> tuple[dict[int, float], dict[int, float | None], dict[int, float]]:
    projected: dict[int, float] = {}
    ages: dict[int, float | None] = {}
    aging_mults: dict[int, float] = {}
    for season in seasons:
        season_age = age + (season - snapshot_year) if age is not None else None
        mult = 1.0
        if apply_aging and season_age is not None and season_age > prime_age:
            mult = max(floor, 1.0 - decline_per_year * (season_age - prime_age))
        fwar = base_fwar * scale * mult
        if cap is not None and fwar > cap:
            fwar = cap
        projected[season] = fwar
        ages[season] = season_age
        aging_mults[season] = mult
    return projected, ages, aging_mults


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


def apply_control_year_fallback(
    seasons: list[int],
    salary_by_season: dict[int, float | None],
    missing_salary_seasons: set[int],
    snapshot_year: int,
    age: float | None,
    control_years: int,
    age_max: int,
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
    return fallback_years, salary_by_season, missing_salary_seasons, True


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
) -> dict[str, Any]:
    config = load_config(config_path)
    base_fwar = player.get("fwar")
    if base_fwar is None:
        base_fwar = 0.0
    else:
        base_fwar = float(base_fwar)
    name_key = normalize_name(player.get("player_name"))
    is_reliever = name_key in reliever_names
    is_two_way = name_key in two_way_names
    player_age = player.get("age")
    weighted_fwar, weighted_meta = compute_weighted_fwar(
        name_key, fwar_weight_seasons, fwar_weights, war_history
    )
    if weighted_fwar is None:
        fwar_source = "snapshot_fwar"
        fwar_pre_regress = base_fwar
    else:
        fwar_source = "weighted_history"
        fwar_pre_regress = weighted_fwar

    is_pitcher = name_key in pitcher_names and not is_two_way
    if is_pitcher and pitcher_regress_weight > 0:
        fwar_post_regress = (
            1.0 - pitcher_regress_weight
        ) * fwar_pre_regress + pitcher_regress_weight * pitcher_regress_target
    else:
        fwar_post_regress = fwar_pre_regress

    reliever_mult_applied = reliever_mult if is_reliever else 1.0
    two_way_mult_applied = two_way_mult if is_two_way else 1.0
    base_fwar = fwar_post_regress * reliever_mult_applied * two_way_mult_applied

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
        if (
            player.get("age") is not None
            and player.get("age") <= control_years_age_max
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
                    "quality_flags": {},
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
        base_fwar,
        seasons,
        fwar_scale_to_use,
        fwar_cap_to_use,
        player_age,
        snapshot_year,
        apply_aging,
        prime_age,
        decline_per_year,
        aging_floor,
    )
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

    override_salary = SPECIAL_SALARY_OVERRIDES_M.get(player.get("player_name"))
    if override_salary is not None and seasons:
        # Treat deferrals as tax AAV for TVP valuation.
        salary_source = "manual_override_aav"
        salary_by_season = {season: float(override_salary) for season in seasons}
        missing_salary_seasons = set()

    control_salary_floor_seasons: set[int] = set()
    if control_fallback_seasons:
        for season in control_fallback_seasons:
            t = season - snapshot_year
            min_salary = config.min_salary_m * ((1.0 + config.min_salary_growth) ** t)
            current_salary = salary_by_season.get(season)
            if current_salary is None:
                salary_by_season[season] = min_salary
                control_salary_floor_seasons.add(season)
                continue
            clamped_salary = max(current_salary, min_salary)
            if clamped_salary != current_salary:
                salary_by_season[season] = clamped_salary
                control_salary_floor_seasons.add(season)

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

    mlb_raw = mlb_result["raw_components"]
    t_to_year = {str(t): season for t, season in enumerate(seasons)}
    guaranteed_fwar_by_t = {str(t): fwar for t, fwar in enumerate(fwar_by_year_base)}
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
            "fwar": opt.get("fwar"),
            "S": opt.get("salary"),
            "B": opt.get("buyout"),
            "V": opt.get("value"),
            "M": opt.get("market"),
            "sigmoid_input_ex": opt.get("sigmoid_input_exercise"),
            "sigmoid_input_in": opt.get("sigmoid_input_in"),
            "P_ex": opt.get("p_ex"),
            "P_in": opt.get("p_in"),
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
        "tvp_mlb": mlb_result["tvp_mlb"],
        "tvp_current": mlb_result["tvp_mlb"],
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
                "guaranteed_projection_by_t": guaranteed_fwar_by_t,
                "option_year_projection_by_t": option_fwar_by_t,
                "aging_mult_by_t": aging_mult_by_t,
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
                "fwar_source": fwar_source_label,
                "war_history_used": weighted_meta.get("seasons"),
                "weighted_fwar": weighted_fwar,
                "is_pitcher": is_pitcher,
                "is_reliever": is_reliever,
                "is_two_way": is_two_way,
                "pitcher_regress_weight": pitcher_regress_weight,
                "pitcher_regress_target": pitcher_regress_target,
                "fwar_pre_regress": fwar_pre_regress,
                "fwar_post_regress": fwar_post_regress,
                "reliever_mult_applied": reliever_mult_applied,
                "two_way_mult_applied": two_way_mult_applied,
                "cap_used": fwar_cap_to_use,
            },
            "projection_scaling_aging": {
                "fwar_scale_used": fwar_scale_to_use,
                "fwar_cap_used": fwar_cap_to_use,
                "aging_mult_by_t": aging_mult_by_t,
                "guaranteed_projection_by_t": guaranteed_fwar_by_t,
                "option_year_projection_by_t": option_fwar_by_t,
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
            "long_control_boost": {
                "boost_applied": long_control_boost_pct > 0,
                "boost_pct": long_control_boost_pct,
                "boost_amount": long_control_boost_value,
            },
            "quality_flags": {
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
                        salary_by_season.get(season) is not None
                        and salary_by_season.get(season)
                        < config.min_salary_m
                        * ((1.0 + config.min_salary_growth) ** (season - snapshot_year))
                        for season in seasons
                    )
                ),
                "guaranteed_includes_option_year": any(
                    season in option_seasons
                    and salary_by_season.get(season) is not None
                    and salary_by_season.get(season) > 0.0
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
    args = parser.parse_args()

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
    fwar_weights = parse_weights(args.fwar_weights)
    if not fwar_weights:
        fwar_weights = [1.0]
    fwar_weight_seasons = [
        snapshot_season - offset for offset in range(len(fwar_weights))
    ]
    war_history = load_war_history(fwar_weight_seasons, stats_db_path)
    contracts_2026_map = load_contracts_2026_map(contracts_2026_path, snapshot_year)
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
