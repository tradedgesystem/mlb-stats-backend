from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def load_stats_config(config_path: Path) -> list[dict]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def export_snapshot(
    year: int,
    output_path: Path,
    config_path: Path,
    table_name: str,
    dataset: str,
) -> None:
    stats_config = load_stats_config(config_path)
    stat_keys = [
        item["key"] for item in stats_config if item.get("available", True)
    ]
    base_keys = ["player_id", "name", "team", "season"]
    columns = base_keys + stat_keys

    db_path = Path(__file__).with_name("stats.db")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        missing = [col for col in columns if col not in existing]
        if missing:
            print(f"Skipping missing columns: {', '.join(missing)}")
        available = [col for col in columns if col in existing]
        quoted = [f'"{col}"' for col in available]
        rows = conn.execute(
            f"SELECT {', '.join(quoted)} FROM {table_name} WHERE season = ?",
            (year,),
        ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    payload = {
        "meta": {
            "generated_at": generated_at,
            "year": year,
            "player_count": len(rows),
            "stat_keys": [key for key in stat_keys if key in available],
            "dataset": dataset,
        },
        "players": [dict(row) for row in rows],
    }
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)

    print(f"Wrote {len(rows)} rows to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export stats snapshot JSON.")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument(
        "--dataset",
        choices=["batting", "pitching"],
        default="batting",
        help="Dataset to export.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    if args.dataset == "pitching":
        config_path = repo_root / "extension" / "pitching_stats_config.json"
        table_name = "pitching_stats"
        default_output = (
            repo_root / "extension" / "snapshots" / f"pitchers_{args.year}.json"
        )
    else:
        config_path = repo_root / "extension" / "stats_config.json"
        table_name = "batting_stats"
        default_output = (
            repo_root / "extension" / "snapshots" / f"players_{args.year}.json"
        )

    output_path = args.output or default_output
    export_snapshot(args.year, output_path, config_path, table_name, args.dataset)


if __name__ == "__main__":
    main()
