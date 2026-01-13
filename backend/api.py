from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

try:
    from .data_utils import parse_date
    from .statcast_metrics import (
        STATCAST_REQUIRED_COLUMNS,
        build_statcast_batter_metrics_from_df,
    )
    from .statcast_range import load_statcast_range
except ImportError:  # pragma: no cover - script execution fallback
    from data_utils import parse_date
    from statcast_metrics import (
        STATCAST_REQUIRED_COLUMNS,
        build_statcast_batter_metrics_from_df,
    )
    from statcast_range import load_statcast_range

DB_PATH = Path(__file__).with_name("stats.db")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ],
    allow_origin_regex=r"chrome-extension://.*",
    allow_methods=["GET"],
    allow_headers=["*"],
)  # Local dev only.

DAILY_BATTING_COLUMNS = [
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

DAILY_PITCHING_COLUMNS = [
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


def parse_player_ids(raw_ids: str, min_count: int = 1, max_count: int = 5) -> list[int]:
    items = [item.strip() for item in raw_ids.split(",") if item.strip()]
    if len(items) < min_count or len(items) > max_count:
        raise HTTPException(
            status_code=400,
            detail=f"player_ids must include {min_count}-{max_count} ids",
        )

    ids: list[int] = []
    for item in items:
        try:
            ids.append(int(item))
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="player_ids must be integers"
            ) from exc
    return ids


def compute_batting_rates(row: dict) -> dict:
    def num(value):
        return float(value) if value is not None else 0.0

    pa = num(row.get("pa"))
    ab = num(row.get("ab"))
    hits = num(row.get("h"))
    doubles = num(row.get("2b"))
    triples = num(row.get("3b"))
    homers = num(row.get("hr"))
    singles = row.get("1b")
    if singles is None:
        singles = hits - doubles - triples - homers
    singles = num(singles)
    walks = num(row.get("bb"))
    hbp = num(row.get("hbp"))
    strikeouts = num(row.get("so"))
    sf = num(row.get("sf"))

    total_bases = singles + 2 * doubles + 3 * triples + 4 * homers

    avg = hits / ab if ab else None
    obp_denom = ab + walks + hbp + sf
    obp = (hits + walks + hbp) / obp_denom if obp_denom else None
    slg = total_bases / ab if ab else None
    ops = (obp + slg) if obp is not None and slg is not None else None
    iso = (slg - avg) if slg is not None and avg is not None else None
    babip_denom = ab - strikeouts - homers + sf
    babip = (hits - homers) / babip_denom if babip_denom else None

    row["1b"] = singles
    row["avg"] = avg
    row["obp"] = obp
    row["slg"] = slg
    row["ops"] = ops
    row["iso"] = iso
    row["babip"] = babip

    if "barrels" in row and pa:
        row["barrels_per_pa"] = num(row.get("barrels")) / pa
    return row


def compute_pitching_rates(row: dict) -> dict:
    def num(value):
        return float(value) if value is not None else 0.0

    ip = num(row.get("ip"))
    hits = num(row.get("h"))
    walks = num(row.get("bb"))
    homers = num(row.get("hr"))
    strikeouts = num(row.get("so"))
    tbf = num(row.get("tbf"))
    er = row.get("er")

    if ip:
        row["whip"] = (hits + walks) / ip
        row["k_9"] = (strikeouts * 9) / ip
        row["bb_9"] = (walks * 9) / ip
        row["hr_9"] = (homers * 9) / ip

    if ip and er is not None:
        row["era"] = (num(er) * 9) / ip

    if tbf:
        row["k_bbpct"] = (strikeouts - walks) / tbf

    return row


def fetch_batting_range(
    conn: sqlite3.Connection,
    year: int,
    start_date,
    end_date,
    player_ids: list[int],
) -> list[dict]:
    placeholders = ",".join("?" for _ in player_ids)
    sums = ", ".join(f'SUM("{col}") AS "{col}"' for col in DAILY_BATTING_COLUMNS)
    query = (
        "SELECT player_id, "
        f"{sums} "
        "FROM batting_stats_daily "
        "WHERE season = ? AND game_date BETWEEN ? AND ? "
        f"AND player_id IN ({placeholders}) "
        "GROUP BY player_id"
    )

    try:
        rows = conn.execute(
            query, (year, start_date.isoformat(), end_date.isoformat(), *player_ids)
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "Date-range data not available. "
                "Run backend/ingest.py with DATE_RANGE_START/DATE_RANGE_END first."
            ),
        ) from exc

    if not rows:
        return []

    name_rows = conn.execute(
        f"SELECT player_id, name, team FROM batting_stats "
        f"WHERE season = ? AND player_id IN ({placeholders})",
        (year, *player_ids),
    ).fetchall()
    name_map = {row["player_id"]: row for row in name_rows}

    results: list[dict] = []
    for row in rows:
        payload = dict(row)
        identity = name_map.get(row["player_id"])
        if identity:
            payload["name"] = identity["name"]
            payload["team"] = identity["team"]
        payload["season"] = year
        payload = compute_batting_rates(payload)
        results.append(payload)
    return results


def fetch_pitching_range(
    conn: sqlite3.Connection,
    year: int,
    start_date,
    end_date,
    player_ids: list[int],
) -> list[dict]:
    placeholders = ",".join("?" for _ in player_ids)
    sums = ", ".join(f'SUM("{col}") AS "{col}"' for col in DAILY_PITCHING_COLUMNS)
    query = (
        "SELECT player_id, "
        f"{sums} "
        "FROM pitching_stats_daily "
        "WHERE season = ? AND game_date BETWEEN ? AND ? "
        f"AND player_id IN ({placeholders}) "
        "GROUP BY player_id"
    )

    try:
        rows = conn.execute(
            query, (year, start_date.isoformat(), end_date.isoformat(), *player_ids)
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "Date-range data not available. "
                "Run backend/ingest.py with DATE_RANGE_START/DATE_RANGE_END first."
            ),
        ) from exc

    if not rows:
        return []

    name_rows = conn.execute(
        f"SELECT player_id, name, team FROM pitching_stats "
        f"WHERE season = ? AND player_id IN ({placeholders})",
        (year, *player_ids),
    ).fetchall()
    name_map = {row["player_id"]: row for row in name_rows}

    results: list[dict] = []
    for row in rows:
        payload = dict(row)
        identity = name_map.get(row["player_id"])
        if identity:
            payload["name"] = identity["name"]
            payload["team"] = identity["team"]
        payload["season"] = year
        payload = compute_pitching_rates(payload)
        results.append(payload)
    return results


@app.get("/players")
def get_players(year: int = Query(..., ge=1800, le=2100)) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM batting_stats WHERE season = ?",
            (year,),
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/pitchers")
def get_pitchers(year: int = Query(..., ge=1800, le=2100)) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM pitching_stats WHERE season = ?",
            (year,),
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/search")
def search_players(
    year: int = Query(..., ge=1800, le=2100),
    q: str = Query(..., min_length=1),
) -> list[dict]:
    term = q.strip()
    if not term:
        return []

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT player_id, name, team "
            "FROM batting_stats "
            "WHERE season = ? AND LOWER(name) LIKE ? "
            "ORDER BY name "
            "LIMIT 50",
            (year, f"%{term.lower()}%"),
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/pitchers/search")
def search_pitchers(
    year: int = Query(..., ge=1800, le=2100),
    q: str = Query(..., min_length=1),
) -> list[dict]:
    term = q.strip()
    if not term:
        return []

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT player_id, name, team "
            "FROM pitching_stats "
            "WHERE season = ? AND LOWER(name) LIKE ? "
            "ORDER BY name "
            "LIMIT 50",
            (year, f"%{term.lower()}%"),
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/compare")
def compare_players(
    year: int = Query(..., ge=1800, le=2100),
    player_ids: str = Query(...),
) -> list[dict]:
    ids = parse_player_ids(player_ids, min_count=2, max_count=5)

    placeholders = ",".join("?" for _ in ids)
    query = (
        "SELECT * FROM batting_stats "
        f"WHERE season = ? AND player_id IN ({placeholders})"
    )

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, (year, *ids)).fetchall()

    rows_by_id = {row["player_id"]: row for row in rows}
    ordered = [dict(rows_by_id[player_id]) for player_id in ids if player_id in rows_by_id]
    return ordered


@app.get("/pitchers/compare")
def compare_pitchers(
    year: int = Query(..., ge=1800, le=2100),
    player_ids: str = Query(...),
) -> list[dict]:
    ids = parse_player_ids(player_ids, min_count=2, max_count=5)

    placeholders = ",".join("?" for _ in ids)
    query = (
        "SELECT * FROM pitching_stats "
        f"WHERE season = ? AND player_id IN ({placeholders})"
    )

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, (year, *ids)).fetchall()

    rows_by_id = {row["player_id"]: row for row in rows}
    ordered = [dict(rows_by_id[player_id]) for player_id in ids if player_id in rows_by_id]
    return ordered


@app.get("/player")
def get_player(
    year: int = Query(..., ge=1800, le=2100),
    player_id: int = Query(..., ge=1),
) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM batting_stats WHERE season = ? AND player_id = ?",
            (year, player_id),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="player not found")

    return dict(row)


@app.get("/pitcher")
def get_pitcher(
    year: int = Query(..., ge=1800, le=2100),
    player_id: int = Query(..., ge=1),
) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM pitching_stats WHERE season = ? AND player_id = ?",
            (year, player_id),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="pitcher not found")

    return dict(row)


@app.get("/players/range")
def get_players_range(
    year: int = Query(..., ge=1800, le=2100),
    start: str = Query(...),
    end: str = Query(...),
    player_ids: str = Query(...),
    include_statcast: bool = Query(False),
) -> list[dict]:
    try:
        start_date = parse_date(start)
        end_date = parse_date(end)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="start/end must be YYYY-MM-DD"
        ) from exc
    if end_date < start_date:
        raise HTTPException(
            status_code=400, detail="end must be on or after start"
        )
    if start_date.year != end_date.year or start_date.year != year:
        raise HTTPException(
            status_code=400,
            detail="Date range must stay within the requested season year.",
        )

    ids = parse_player_ids(player_ids, min_count=1, max_count=10)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        range_rows = fetch_batting_range(conn, year, start_date, end_date, ids)

    if not range_rows:
        return []

    if include_statcast:
        statcast_df = load_statcast_range(
            season=year,
            start_date=start_date,
            end_date=end_date,
            player_ids=ids,
            columns=sorted(STATCAST_REQUIRED_COLUMNS),
        )
        if not statcast_df.empty:
            metrics = build_statcast_batter_metrics_from_df(statcast_df)
            metrics_by_id = {
                row["player_id"]: row for row in metrics.to_dict("records")
            }
            for row in range_rows:
                extra = metrics_by_id.get(row["player_id"])
                if extra:
                    row.update(extra)
                if row.get("barrels") is not None and row.get("pa"):
                    row["barrels_per_pa"] = float(row["barrels"]) / float(row["pa"])

    return range_rows


@app.get("/pitchers/range")
def get_pitchers_range(
    year: int = Query(..., ge=1800, le=2100),
    start: str = Query(...),
    end: str = Query(...),
    player_ids: str = Query(...),
) -> list[dict]:
    try:
        start_date = parse_date(start)
        end_date = parse_date(end)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="start/end must be YYYY-MM-DD"
        ) from exc
    if end_date < start_date:
        raise HTTPException(
            status_code=400, detail="end must be on or after start"
        )
    if start_date.year != end_date.year or start_date.year != year:
        raise HTTPException(
            status_code=400,
            detail="Date range must stay within the requested season year.",
        )

    ids = parse_player_ids(player_ids, min_count=1, max_count=10)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        range_rows = fetch_pitching_range(conn, year, start_date, end_date, ids)

    return range_rows
