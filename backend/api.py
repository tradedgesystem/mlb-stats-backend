from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, Query

DB_PATH = Path(__file__).with_name("stats.db")

app = FastAPI()


@app.get("/players")
def get_players(year: int = Query(..., ge=1800, le=2100)) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM batting_stats WHERE season = ?",
            (year,),
        ).fetchall()

    return [dict(row) for row in rows]
