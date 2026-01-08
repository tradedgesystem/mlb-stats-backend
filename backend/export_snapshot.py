from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def load_stats_config(config_path: Path) -> list[dict]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def export_snapshot(year: int, output_path: Path, config_path: Path) -> None:
    stats_config = load_stats_config(config_path)
    stat_keys = [item["key"] for item in stats_config]
    base_keys = ["player_id", "name", "team", "season"]
    columns = base_keys + stat_keys

    db_path = Path(__file__).with_name("stats.db")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(batting_stats)").fetchall()
        }
        missing = [col for col in columns if col not in existing]
        if missing:
            print(f"Skipping missing columns: {', '.join(missing)}")
        available = [col for col in columns if col in existing]
        quoted = [f'"{col}"' for col in available]
        rows = conn.execute(
            f"SELECT {', '.join(quoted)} FROM batting_stats WHERE season = ?",
            (year,),
        ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump([dict(row) for row in rows], handle, ensure_ascii=True)

    print(f"Wrote {len(rows)} rows to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export stats snapshot JSON.")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "extension" / "stats_config.json"

    output_path = args.output
    if output_path is None:
        output_path = Path(__file__).with_name("snapshots") / f"players_{args.year}.json"

    export_snapshot(args.year, output_path, config_path)


if __name__ == "__main__":
    main()
