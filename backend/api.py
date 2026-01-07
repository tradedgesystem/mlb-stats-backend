from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

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


@app.get("/players")
def get_players(year: int = Query(..., ge=1800, le=2100)) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM batting_stats WHERE season = ?",
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


@app.get("/compare")
def compare_players(
    year: int = Query(..., ge=1800, le=2100),
    player_ids: str = Query(...),
) -> list[dict]:
    raw_ids = [item.strip() for item in player_ids.split(",") if item.strip()]
    if len(raw_ids) < 2 or len(raw_ids) > 5:
        raise HTTPException(status_code=400, detail="player_ids must include 2-5 ids")

    ids: list[int] = []
    for item in raw_ids:
        try:
            ids.append(int(item))
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="player_ids must be integers"
            ) from exc

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
