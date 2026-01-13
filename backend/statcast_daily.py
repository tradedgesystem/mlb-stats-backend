from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd

try:
    from pybaseball import playerid_reverse_lookup
except ImportError:  # pragma: no cover - script execution fallback
    playerid_reverse_lookup = None

try:
    from .data_utils import iter_dates
    from .statcast_range import DEFAULT_RAW_ROOT
except ImportError:  # pragma: no cover - script execution fallback
    from data_utils import iter_dates
    from statcast_range import DEFAULT_RAW_ROOT

STATCAST_DAILY_COLUMNS = [
    "player_id",
    "pitcher",
    "events",
    "inning_topbot",
    "home_team",
    "away_team",
]

HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
HBP_EVENTS = {"hit_by_pitch"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
SAC_FLY_EVENTS = {"sac_fly", "sac_fly_double_play"}
SAC_BUNT_EVENTS = {"sac_bunt", "sac_bunt_double_play"}
NON_AB_EVENTS = WALK_EVENTS | HBP_EVENTS | SAC_FLY_EVENTS | SAC_BUNT_EVENTS | {
    "catcher_interference"
}
INVALID_PA_EVENTS = {"truncated_pa"}

OUTS_BY_EVENT = {
    "field_out": 1,
    "force_out": 1,
    "fielders_choice_out": 1,
    "strikeout": 1,
    "sac_fly": 1,
    "sac_bunt": 1,
    "double_play": 2,
    "grounded_into_double_play": 2,
    "strikeout_double_play": 2,
    "sac_fly_double_play": 2,
    "sac_bunt_double_play": 2,
    "triple_play": 3,
}


def load_statcast_day(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, columns=STATCAST_DAILY_COLUMNS)


def pick_team(series: pd.Series) -> str | None:
    values = series.dropna()
    if values.empty:
        return None
    mode = values.mode()
    if not mode.empty:
        return str(mode.iloc[0])
    return str(values.iloc[0])

def map_pitcher_ids(statcast_df: pd.DataFrame, id_cache: dict[int, int]) -> pd.DataFrame:
    if "pitcher" not in statcast_df.columns:
        return statcast_df
    if playerid_reverse_lookup is None:
        return statcast_df

    pitcher_ids = (
        pd.to_numeric(statcast_df["pitcher"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    missing = [pid for pid in pitcher_ids if pid not in id_cache]
    if missing:
        lookup = playerid_reverse_lookup(missing, key_type="mlbam")
        if not lookup.empty and "key_fangraphs" in lookup.columns:
            lookup = lookup.dropna(subset=["key_mlbam", "key_fangraphs"])
            mapping = lookup.set_index("key_mlbam")["key_fangraphs"].to_dict()
            id_cache.update({int(k): int(v) for k, v in mapping.items()})

    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["pitcher"], errors="coerce"
    ).map(id_cache)
    missing_count = statcast_df["player_id"].isna().sum()
    if missing_count:
        print(f"Statcast mapping missing pitcher IDs for {missing_count} rows.")
    return statcast_df


def aggregate_batting_day(
    statcast_df: pd.DataFrame, season: int, day: datetime.date
) -> pd.DataFrame:
    if statcast_df.empty:
        columns = [
            "player_id",
            "name",
            "team",
            "season",
            "game_date",
            "pa",
            "ab",
            "h",
            "1b",
            "2b",
            "3b",
            "hr",
            "r",
            "rbi",
            "bb",
            "ibb",
            "hbp",
            "so",
            "sf",
            "sh",
        ]
        return pd.DataFrame(columns=columns)

    statcast_df = statcast_df.copy()
    statcast_df = statcast_df[statcast_df["events"].notna()].copy()
    if statcast_df.empty:
        return aggregate_batting_day(pd.DataFrame(), season, day)

    statcast_df = statcast_df[~statcast_df["events"].isin(INVALID_PA_EVENTS)]
    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["player_id"], errors="coerce"
    )
    statcast_df = statcast_df[statcast_df["player_id"].notna()].copy()
    if statcast_df.empty:
        return aggregate_batting_day(pd.DataFrame(), season, day)

    statcast_df["player_id"] = statcast_df["player_id"].astype(int)

    if {"inning_topbot", "home_team", "away_team"}.issubset(statcast_df.columns):
        top_mask = statcast_df["inning_topbot"].str.startswith("Top", na=False)
        statcast_df["team"] = statcast_df["away_team"].where(
            top_mask, statcast_df["home_team"]
        )
    else:
        statcast_df["team"] = pd.NA

    events = statcast_df["events"]
    player_index = statcast_df.groupby("player_id").size().index
    counts = pd.DataFrame(index=player_index)
    counts["pa"] = statcast_df.groupby("player_id").size()

    def count(mask: pd.Series) -> pd.Series:
        return (
            statcast_df.loc[mask].groupby("player_id").size().reindex(player_index, fill_value=0)
        )

    counts["1b"] = count(events == "single")
    counts["2b"] = count(events == "double")
    counts["3b"] = count(events == "triple")
    counts["hr"] = count(events == "home_run")
    counts["h"] = count(events.isin(HIT_EVENTS))
    counts["bb"] = count(events.isin(WALK_EVENTS))
    counts["ibb"] = count(events == "intent_walk")
    counts["hbp"] = count(events.isin(HBP_EVENTS))
    counts["so"] = count(events.isin(STRIKEOUT_EVENTS))
    counts["sf"] = count(events.isin(SAC_FLY_EVENTS))
    counts["sh"] = count(events.isin(SAC_BUNT_EVENTS))
    counts["ab"] = count(~events.isin(NON_AB_EVENTS))
    counts["r"] = pd.NA
    counts["rbi"] = pd.NA

    teams = statcast_df.groupby("player_id")["team"].agg(pick_team)

    counts = counts.reset_index()
    counts["name"] = pd.NA
    counts["team"] = counts["player_id"].map(teams)
    counts["season"] = season
    counts["game_date"] = day.isoformat()

    ordered = [
        "player_id",
        "name",
        "team",
        "season",
        "game_date",
        "pa",
        "ab",
        "h",
        "1b",
        "2b",
        "3b",
        "hr",
        "r",
        "rbi",
        "bb",
        "ibb",
        "hbp",
        "so",
        "sf",
        "sh",
    ]
    return counts[ordered]


def aggregate_pitching_day(
    statcast_df: pd.DataFrame,
    season: int,
    day: datetime.date,
    id_cache: dict[int, int],
) -> pd.DataFrame:
    if statcast_df.empty:
        columns = [
            "player_id",
            "name",
            "team",
            "season",
            "game_date",
            "ip",
            "tbf",
            "h",
            "r",
            "er",
            "hr",
            "bb",
            "hbp",
            "so",
        ]
        return pd.DataFrame(columns=columns)

    statcast_df = statcast_df.copy()
    statcast_df = statcast_df[statcast_df["events"].notna()].copy()
    if statcast_df.empty:
        return aggregate_pitching_day(pd.DataFrame(), season, day, id_cache)

    statcast_df = statcast_df[~statcast_df["events"].isin(INVALID_PA_EVENTS)]
    statcast_df = map_pitcher_ids(statcast_df, id_cache)
    statcast_df = statcast_df[statcast_df["player_id"].notna()].copy()
    if statcast_df.empty:
        return aggregate_pitching_day(pd.DataFrame(), season, day, id_cache)

    statcast_df["player_id"] = statcast_df["player_id"].astype(int)

    if {"inning_topbot", "home_team", "away_team"}.issubset(statcast_df.columns):
        top_mask = statcast_df["inning_topbot"].str.startswith("Top", na=False)
        statcast_df["team"] = statcast_df["home_team"].where(
            top_mask, statcast_df["away_team"]
        )
    else:
        statcast_df["team"] = pd.NA

    events = statcast_df["events"]
    player_index = statcast_df.groupby("player_id").size().index
    counts = pd.DataFrame(index=player_index)

    def count(mask: pd.Series) -> pd.Series:
        return (
            statcast_df.loc[mask].groupby("player_id").size().reindex(player_index, fill_value=0)
        )

    outs = events.map(OUTS_BY_EVENT).fillna(0).astype(float)
    counts["tbf"] = statcast_df.groupby("player_id").size()
    counts["ip"] = (
        statcast_df.assign(outs=outs)
        .groupby("player_id")["outs"]
        .sum()
        .div(3)
    )
    counts["h"] = count(events.isin(HIT_EVENTS))
    counts["hr"] = count(events == "home_run")
    counts["bb"] = count(events.isin(WALK_EVENTS))
    counts["hbp"] = count(events.isin(HBP_EVENTS))
    counts["so"] = count(events.isin(STRIKEOUT_EVENTS))
    counts["r"] = pd.NA
    counts["er"] = pd.NA

    teams = statcast_df.groupby("player_id")["team"].agg(pick_team)

    counts = counts.reset_index()
    counts["name"] = pd.NA
    counts["team"] = counts["player_id"].map(teams)
    counts["season"] = season
    counts["game_date"] = day.isoformat()

    ordered = [
        "player_id",
        "name",
        "team",
        "season",
        "game_date",
        "ip",
        "tbf",
        "h",
        "r",
        "er",
        "hr",
        "bb",
        "hbp",
        "so",
    ]
    return counts[ordered]


def merge_mlb_gamelogs(
    statcast_df: pd.DataFrame, gamelog_df: pd.DataFrame
) -> pd.DataFrame:
    if gamelog_df.empty or statcast_df.empty:
        return statcast_df

    merged = statcast_df.merge(
        gamelog_df,
        on=["player_id", "game_date"],
        how="left",
        suffixes=("", "_gamelog"),
    )

    for col in ["r", "rbi"]:
        if col + "_gamelog" in merged.columns:
            merged[col] = merged[col + "_gamelog"]
            merged = merged.drop(columns=[col + "_gamelog"])

    return merged


def build_daily_batting_from_statcast(
    start_date: datetime.date,
    end_date: datetime.date,
    base_dir: Path = DEFAULT_RAW_ROOT,
    gamelog_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day in iter_dates(start_date, end_date):
        season = day.year
        path = (
            base_dir
            / f"season={season}"
            / f"game_date={day.isoformat()}"
            / "statcast.parquet"
        )
        if not path.exists():
            continue
        daily_raw = load_statcast_day(path)
        frames.append(aggregate_batting_day(daily_raw, season, day))

    if not frames:
        return aggregate_batting_day(pd.DataFrame(), start_date.year, start_date)
    
    result = pd.concat(frames, ignore_index=True)
    
    if gamelog_df is not None and not gamelog_df.empty:
        result = merge_mlb_gamelogs(result, gamelog_df)
    
    return result


def build_daily_pitching_from_statcast(
    start_date: datetime.date,
    end_date: datetime.date,
    base_dir: Path = DEFAULT_RAW_ROOT,
    id_cache: dict[int, int] | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    id_cache = id_cache or {}
    for day in iter_dates(start_date, end_date):
        season = day.year
        path = (
            base_dir
            / f"season={season}"
            / f"game_date={day.isoformat()}"
            / "statcast.parquet"
        )
        if not path.exists():
            continue
        daily_raw = load_statcast_day(path)
        frames.append(aggregate_pitching_day(daily_raw, season, day, id_cache))

    if not frames:
        return aggregate_pitching_day(pd.DataFrame(), start_date.year, start_date, id_cache)
    return pd.concat(frames, ignore_index=True)
