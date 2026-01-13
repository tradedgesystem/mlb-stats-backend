from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from .data_utils import iter_dates
except ImportError:  # pragma: no cover - script execution fallback
    from data_utils import iter_dates

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover - optional acceleration
    duckdb = None


DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[1] / "data" / "statcast" / "raw"


def statcast_paths_for_range(
    base_dir: Path,
    season: int,
    start_date,
    end_date,
) -> list[Path]:
    paths: list[Path] = []
    for day in iter_dates(start_date, end_date):
        path = base_dir / f"season={season}" / f"game_date={day.isoformat()}" / "statcast.parquet"
        if path.exists():
            paths.append(path)
    return paths


def load_statcast_range(
    *,
    season: int,
    start_date,
    end_date,
    player_ids: Iterable[int] | None = None,
    columns: list[str] | None = None,
    base_dir: Path = DEFAULT_RAW_ROOT,
) -> pd.DataFrame:
    paths = statcast_paths_for_range(base_dir, season, start_date, end_date)
    if not paths:
        return pd.DataFrame(columns=columns or [])

    columns = columns or None
    if columns and "player_id" not in columns:
        columns = ["player_id"] + columns

    player_ids = list(player_ids or [])

    if duckdb:
        con = duckdb.connect()
        rel = con.read_parquet([str(path) for path in paths])
        if columns:
            rel = rel.project(", ".join(columns))
        if player_ids:
            ids = ", ".join(str(pid) for pid in player_ids)
            rel = rel.filter(f"player_id IN ({ids})")
        df = rel.df()
        con.close()
        return df

    frames = [pd.read_parquet(path, columns=columns) for path in paths]
    df = pd.concat(frames, ignore_index=True)
    if player_ids:
        df = df[df["player_id"].isin(player_ids)]
    return df
