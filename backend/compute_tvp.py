from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from tvp_engine import compute_prospect_tvp, load_config


def load_latest_prospects(repo_root: Path) -> Path:
    prospects_dir = repo_root / "backend" / "data" / "prospects_cache"
    files = sorted(prospects_dir.glob("all_prospects_*.json"))
    if not files:
        raise FileNotFoundError("No all_prospects_*.json files found.")
    return files[-1]


def flatten_prospects(raw: dict) -> list[dict]:
    prospects = []
    for team_players in raw.values():
        prospects.extend(team_players)
    return prospects


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute TVP for prospect data.")
    parser.add_argument("--config", type=Path, help="Path to tvp_config.json")
    parser.add_argument("--prospects", type=Path, help="Path to all_prospects_*.json")
    parser.add_argument("--output", type=Path, help="Output JSON path")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    config_path = args.config or (repo_root / "backend" / "tvp_config.json")
    prospects_path = args.prospects or load_latest_prospects(repo_root)

    config = load_config(config_path)
    with prospects_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    prospects = flatten_prospects(raw)

    results = [compute_prospect_tvp(prospect, config) for prospect in prospects]

    generated_at = (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    output_path = args.output or (
        repo_root
        / "backend"
        / "output"
        / f"tvp_prospects_{generated_at.replace(':', '').replace('-', '')}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "generated_at": generated_at,
            "snapshot_year": config.snapshot_year,
            "prospect_count": len(results),
            "source_file": str(prospects_path),
        },
        "prospects": results,
    }

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)

    print(f"Wrote {len(results)} prospects to {output_path}")


if __name__ == "__main__":
    main()
