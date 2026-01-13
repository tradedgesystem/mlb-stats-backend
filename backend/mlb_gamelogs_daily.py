from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    from pybaseball import playerid_reverse_lookup
except ImportError:
    playerid_reverse_lookup = None

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
COMPLETED_GAME_STATES = {"Final", "Game Over", "Completed Early"}
DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "mlb_api"


def get_cache_dir(cache_dir: Path | None = None) -> Path:
    if cache_dir:
        return Path(cache_dir)
    env_dir = os.environ.get("CACHE_DIR")
    if env_dir:
        return Path(env_dir)
    return DEFAULT_CACHE_DIR


def load_id_map(cache_dir: Path) -> dict[int, int]:
    path = cache_dir / "id_map_mlbam_to_idfg.json"
    if path.exists():
        with open(path, "r") as f:
            data = json.load(f)
            return {int(k): v for k, v in data.items()}
    return {}


def save_id_map(cache_dir: Path, id_map: dict[int, int]) -> None:
    path = cache_dir / "id_map_mlbam_to_idfg.json"
    with open(path, "w") as f:
        json.dump({str(k): v for k, v in id_map.items()}, f)


def fetch_schedule(
    start_date_str: str, end_date_str: str
) -> tuple[list[int], dict[int, str]]:
    params = {
        "sportId": "1",
        "startDate": start_date_str,
        "endDate": end_date_str,
    }
    response = requests.get(SCHEDULE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    game_pks: list[int] = []
    game_dates: dict[int, str] = {}
    if "dates" not in data:
        return game_pks, game_dates

    for date_data in data["dates"]:
        if "games" not in date_data:
            continue
        game_date = date_data.get("date", "")
        for game in date_data["games"]:
            status = game.get("status", {})
            detailed_state = status.get("detailedState", "")
            if detailed_state in COMPLETED_GAME_STATES:
                game_pk = game["gamePk"]
                game_pks.append(game_pk)
                game_dates[game_pk] = game_date

    return game_pks, game_dates


def fetch_boxscore(
    game_pk: int, cache_dir: Path, dry_run: bool = False
) -> dict[str, Any] | None:
    cache_path = cache_dir / "boxscore" / f"{game_pk}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)

    if dry_run:
        print(f"Warning: Boxscore cache missing for game {game_pk}, skipping (dry-run)")
        return None

    url = BOXSCORE_URL.format(game_pk)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    with open(cache_path, "w") as f:
        json.dump(data, f)

    return data


def parse_boxscore(
    boxscore: dict[str, Any] | None, game_date: str
) -> tuple[pd.DataFrame, list[int]]:
    if boxscore is None:
        return pd.DataFrame(columns=["player_id", "game_date", "r", "rbi"]), []

    rows = []
    mlb_ids = []
    teams = boxscore.get("teams", {})
    for team_side in ["home", "away"]:
        team_data = teams.get(team_side, {})
        players_data = team_data.get("players", {})

        for player_key, player in players_data.items():
            if not isinstance(player, dict):
                continue
            mlb_id = player.get("person", {}).get("id")
            if not mlb_id:
                continue

            mlb_ids.append(mlb_id)

            stats = player.get("stats", {})
            batting_stats = stats.get("batting", {})

            runs = batting_stats.get("runs", 0)
            rbi = batting_stats.get("rbi", 0)

            rows.append({
                "mlb_id": mlb_id,
                "game_date": game_date,
                "r": runs,
                "rbi": rbi,
            })

    if not rows:
        return pd.DataFrame(columns=["player_id", "game_date", "r", "rbi"]), []

    df = pd.DataFrame(rows)
    return df, mlb_ids


def build_id_mapping(
    mlb_ids: list[int], cache_dir: Path, id_map: dict[int, int]
) -> dict[int, int]:
    missing = [mlb_id for mlb_id in mlb_ids if mlb_id not in id_map]
    if not missing:
        return id_map

    if playerid_reverse_lookup is None:
        print("Warning: pybaseball not available, cannot map MLBAM IDs")
        return id_map

    try:
        lookup = playerid_reverse_lookup(missing, key_type="mlbam")
    except Exception as exc:
        print(f"Warning: Failed to lookup player IDs: {exc}")
        return id_map

    if lookup.empty or "key_fangraphs" not in lookup.columns:
        return id_map

    lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
    mapping = lookup.set_index("key_mlbam")["key_fangraphs"].to_dict()
    
    for mlb_id, fg_id in mapping.items():
        id_map[int(mlb_id)] = int(fg_id)

    save_id_map(cache_dir, id_map)
    return id_map


def fetch_gamelogs(
    start_date_str: str,
    end_date_str: str,
    cache_dir: Path | None = None,
    dry_run: bool = False,
    sleep_seconds: float = 1.0,
) -> pd.DataFrame:
    cache_dir = get_cache_dir(cache_dir)
    id_map = load_id_map(cache_dir)

    game_pks, game_dates = fetch_schedule(start_date_str, end_date_str)
    if not game_pks:
        return pd.DataFrame(columns=["player_id", "game_date", "r", "rbi"])

    print(f"Found {len(game_pks)} completed games in date range")

    all_rows = []
    mlb_ids_seen = set()

    for i, game_pk in enumerate(game_pks, 1):
        print(f"Fetching boxscore {i}/{len(game_pks)}: game {game_pk}")
        
        boxscore = fetch_boxscore(game_pk, cache_dir, dry_run)
        if boxscore is None:
            continue

        game_date = game_dates.get(game_pk, "")
        if not game_date:
            continue

        df, mlb_ids = parse_boxscore(boxscore, game_date)
        if not df.empty:
            all_rows.append(df)
            mlb_ids_seen.update(mlb_ids)

        if sleep_seconds > 0 and i < len(game_pks):
            time.sleep(sleep_seconds)

    if not all_rows:
        return pd.DataFrame(columns=["player_id", "game_date", "r", "rbi"])

    combined = pd.concat(all_rows, ignore_index=True)

    mlb_ids = list(mlb_ids_seen)
    if mlb_ids:
        print(f"Building ID mapping for {len(mlb_ids)} players...")
        id_map = build_id_mapping(mlb_ids, cache_dir, id_map)

    combined["player_id"] = combined["mlb_id"].map(id_map)
    combined = combined[combined["player_id"].notna()]
    combined["player_id"] = combined["player_id"].astype(int)
    combined = combined.drop(columns=["mlb_id"])

    result = combined[["player_id", "game_date", "r", "rbi"]].drop_duplicates(
        subset=["player_id", "game_date"], keep="last"
    )

    return result
