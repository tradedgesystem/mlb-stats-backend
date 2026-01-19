from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from compute_mlb_tvp import (  # noqa: E402
    adjust_player_age,
    attach_positions,
    build_catcher_ids,
    compute_player_tvp,
    ensure_positions_map,
    enrich_players,
    load_contracts_2026_map,
    load_pitcher_names,
    load_players,
    load_reliever_names,
    load_prospect_anchors,
    load_sample_counts,
    load_two_way_names,
    load_war_history,
    normalize_name,
    parse_weights,
)
from tvp_engine import load_config  # noqa: E402


def parse_player_list(raw: str) -> list[str]:
    return [name.strip() for name in raw.split(",") if name.strip()]


def parse_mlb_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    ids: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        ids.append(int(item))
    return ids


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower())
    return slug.strip("_")


def select_players(
    players: list[dict[str, Any]], names: list[str], mlb_ids: list[int]
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    if mlb_ids:
        if len(mlb_ids) != len(names):
            raise ValueError("Length of --mlb-ids must match --players.")
        for name, mlb_id in zip(names, mlb_ids):
            matches = [p for p in players if p.get("mlb_id") == mlb_id]
            if len(matches) != 1:
                raise ValueError(
                    f"Could not resolve {name} with mlb_id={mlb_id}."
                )
            selected.append(matches[0])
        return selected

    for name in names:
        exact = [p for p in players if p.get("player_name") == name]
        if len(exact) == 1:
            selected.append(exact[0])
            continue
        if len(exact) > 1:
            ids = [p.get("mlb_id") for p in exact]
            raise ValueError(
                f"Ambiguous name '{name}' (mlb_ids={ids}). Use --mlb-ids."
            )
        lower = [p for p in players if str(p.get("player_name", "")).lower() == name.lower()]
        if len(lower) == 1:
            selected.append(lower[0])
            continue
        if len(lower) > 1:
            ids = [p.get("mlb_id") for p in lower]
            raise ValueError(
                f"Ambiguous name '{name}' (mlb_ids={ids}). Use --mlb-ids."
            )
        name_key = normalize_name(name)
        normalized = [
            p for p in players if normalize_name(p.get("player_name")) == name_key
        ]
        if len(normalized) == 1:
            selected.append(normalized[0])
            continue
        ids = [p.get("mlb_id") for p in normalized]
        raise ValueError(
            f"Could not resolve '{name}' uniquely (mlb_ids={ids}). Use --mlb-ids."
        )
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump MLB TVP raw_components for selected players."
    )
    parser.add_argument(
        "--players",
        required=True,
        help='Comma-separated player names, e.g. "Cal Raleigh,Tyler Soderstrom".',
    )
    parser.add_argument(
        "--mlb-ids",
        help="Optional comma-separated MLB IDs to disambiguate names (same order).",
    )
    parser.add_argument("--config", type=Path, help="Path to tvp_config.json")
    parser.add_argument(
        "--players-file",
        type=Path,
        help="Path to players_with_contracts_*.json",
    )
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
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "out",
        help="Directory to write raw_components JSON files.",
    )
    args = parser.parse_args()

    config_path = args.config or (REPO_ROOT / "backend" / "tvp_config.json")
    players_path = args.players_file or (
        REPO_ROOT / "backend" / "output" / "players_with_contracts_2025.json"
    )
    payload = load_players(players_path)
    positions_path = REPO_ROOT / "backend" / "data" / "player_positions.json"
    positions_map, positions_missing = ensure_positions_map(
        positions_path,
        players_path,
        allow_missing=True,
        no_position_refresh=False,
    )
    position_by_id = attach_positions(payload.get("players", []), positions_map)
    catcher_ids = build_catcher_ids(position_by_id)
    snapshot_year = load_config(config_path).snapshot_year
    snapshot_season = payload.get("meta", {}).get("season", snapshot_year)
    age_offset = 0
    if isinstance(snapshot_season, int):
        age_offset = max(0, snapshot_year - snapshot_season)

    stats_db_path = REPO_ROOT / "backend" / "stats.db"
    contracts_2026_path = REPO_ROOT / "mlb_2026_contracts_all_teams.sqlite"
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
    sample_counts = load_sample_counts(snapshot_season, stats_db_path)
    prospects_by_id, prospects_by_name = load_prospect_anchors(REPO_ROOT)
    contracts_2026_map = load_contracts_2026_map(contracts_2026_path, snapshot_year)
    enrich_players(
        payload.get("players", []),
        sample_counts,
        prospects_by_id,
        prospects_by_name,
    )

    names = parse_player_list(args.players)
    mlb_ids = parse_mlb_ids(args.mlb_ids)
    players = select_players(payload.get("players", []), names, mlb_ids)
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
        )
        for player in players
    ]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for result in results:
        name = result.get("player_name") or "player"
        mlb_id = result.get("mlb_id")
        slug = slugify(name)
        path = args.output_dir / f"raw_components_{slug}_{mlb_id}.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=True, indent=2)

    combined_path = args.output_dir / "raw_components_top3.json"
    with combined_path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=True, indent=2)

    print("\n=== CATCHER PROJECTION FIELDS ===")
    for result in results:
        projection = result.get("raw_components", {}).get("projection", {})
        print(f"{result.get('player_name')} ({result.get('mlb_id')})")
        print(f"  position: {projection.get('position')}")
        print(f"  position_source: {projection.get('position_source')}")
        print(f"  is_catcher: {projection.get('is_catcher')}")

    print(f"Wrote {len(results)} players to {args.output_dir}")


if __name__ == "__main__":
    main()
