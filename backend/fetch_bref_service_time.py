#!/usr/bin/env python3
"""
Fetch MLB service time from Baseball-Reference player pages.

Outputs:
- backend/output/bref_service_time_cache.json (cache keyed by bbref_id)
- backend/output/service_time_bref_2026_under6.json (players with < 6 years)
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pybaseball import playerid_reverse_lookup

DB_PATH = Path(__file__).with_name("stats.db")
OUTPUT_DIR = Path(__file__).with_name("output")
CACHE_FILE = OUTPUT_DIR / "bref_service_time_cache.json"
OUTPUT_FILE = OUTPUT_DIR / "service_time_bref_2026_under6.json"
CHECKPOINT_FILE = OUTPUT_DIR / "service_time_resume_checkpoint.json"
DEFAULT_SOURCE = "baseball_reference"

SERVICE_TIME_RE = re.compile(r"Service Time \(([^)]+)\)</strong>:\s*([0-9]+)\.([0-9]{3})")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class ServiceTime:
    label: str
    years: int
    days: int

    @property
    def raw(self) -> str:
        return f"{self.years}.{self.days:03d}"


def parse_service_time(html: str) -> Optional[ServiceTime]:
    match = SERVICE_TIME_RE.search(html)
    if not match:
        return None
    label, years_str, days_str = match.groups()
    try:
        years = int(years_str)
        days = int(days_str)
    except ValueError:
        return None
    return ServiceTime(label=label, years=years, days=days)


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    with CACHE_FILE.open() as f:
        return json.load(f)


def save_cache(cache: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with CACHE_FILE.open("w") as f:
        json.dump(cache, f, indent=2)


def fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def load_mlbam_ids_from_db(db_path: Path) -> list[int]:
    ids: set[int] = set()
    with sqlite3.connect(db_path) as conn:
        for table, col in (("batting_stats", "mlbid"), ("pitching_stats", "mlbid")):
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL"
            ).fetchall()
            for (val,) in rows:
                if val is None:
                    continue
                try:
                    ids.add(int(val))
                except (TypeError, ValueError):
                    continue
    return sorted(ids)


def load_missing_service_ids(db_path: Path) -> list[int]:
    all_ids = load_mlbam_ids_from_db(db_path)
    missing = set(all_ids)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT mlbam_id, service_time_years, service_time_days "
            "FROM service_time_bref"
        ).fetchall()
        for mlbam_id, years, days in rows:
            if mlbam_id is None:
                continue
            try:
                mlbam_id = int(mlbam_id)
            except (TypeError, ValueError):
                continue
            years = int(years or 0)
            days = int(days or 0)
            if years > 0 or days > 0:
                missing.discard(mlbam_id)
    return sorted(missing)


def load_checkpoint(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def save_checkpoint(path: Path, remaining_ids: list[int], index: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump({"remaining_ids": remaining_ids, "index": index}, f, indent=2)


def compute_service_time_summary(
    db_path: Path,
    snapshot_date: str | None,
    war_source: str,
    top_n: int,
    rank_by: str | None,
    include_small_sample: bool,
    max_requests: int | None,
    remaining_ids: list[int],
    checkpoint_index: int,
) -> None:
    from datetime import date as dt_date
    from backend.compute_mlb_tvp import (
        build_player_output,
        build_snapshot_players,
        load_config,
    )
    from backend.service_time import SeasonWindow, remaining_games_fraction, super_two_for_snapshot, ServiceTimeRecord

    snapshot = (
        dt_date.fromisoformat(snapshot_date)
        if snapshot_date
        else dt_date.today()
    )
    config = load_config(Path(__file__).with_name("tvp_config.json"), war_source)
    data_dir = Path(__file__).with_name("output")
    players = build_snapshot_players(snapshot.year, war_source, data_dir, db_path, config)
    season_window = SeasonWindow(
        start=dt_date(snapshot.year, config.season_window.start.month, config.season_window.start.day),
        end=dt_date(snapshot.year, config.season_window.end.month, config.season_window.end.day),
    )
    frac = remaining_games_fraction(snapshot, season_window)
    service_records = [p.get("service_time") for p in players if p.get("service_time")]
    service_records = [rec for rec in service_records if isinstance(rec, ServiceTimeRecord)]
    super_two = super_two_for_snapshot(service_records, snapshot, season_window)

    outputs = []
    for player in players:
        out = build_player_output(player, config, snapshot.year, frac, super_two.super_two_ids)
        if out:
            outputs.append(out)

    rank_metric = rank_by or config.leaderboard_rank_by
    rank_key = (lambda o: o.tvp_risk_adj) if rank_metric == "tvp_risk_adj" else (lambda o: o.tvp_p50)
    outputs_sorted = sorted(outputs, key=rank_key, reverse=True)
    leaderboard_pool = [o for o in outputs_sorted if o.flags.get("leaderboard_eligible", True)]
    if include_small_sample:
        eligible_outputs = leaderboard_pool
    else:
        eligible_outputs = [o for o in leaderboard_pool if not o.flags.get("small_sample", False)]
    top = eligible_outputs[:top_n]

    zero_all = sum(
        1 for o in outputs_sorted if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    zero_lb = sum(
        1 for o in leaderboard_pool if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    zero_top = sum(
        1 for o in top if (o.service_time is None or o.service_time in {"0", "0/000", "00/000"})
    )
    pct_all = zero_all / len(outputs_sorted) if outputs_sorted else 0.0
    pct_lb = zero_lb / len(leaderboard_pool) if leaderboard_pool else 0.0
    pct_top = zero_top / len(top) if top else 0.0
    batch = max_requests or 0
    batches_needed = ((len(remaining_ids) + batch - 1) // batch) if batch else None

    print("Summary:")
    print(f"  remaining_ids: {len(remaining_ids)}")
    print(f"  checkpoint_index: {checkpoint_index}")
    print(f"  service_time_zero_pct_all: {pct_all:.4f}")
    print(f"  service_time_zero_pct_leaderboard: {pct_lb:.4f}")
    print(f"  service_time_zero_pct_top50: {pct_top:.4f}")
    if batches_needed is not None:
        print(f"  batches_needed: {batches_needed}")


def map_to_bbref_ids(mlbam_ids: Iterable[int]) -> dict[int, str]:
    if not mlbam_ids:
        return {}
    lookup = playerid_reverse_lookup(list(mlbam_ids), key_type="mlbam")
    if lookup.empty or "key_bbref" not in lookup.columns:
        return {}
    mapping = {}
    for _, row in lookup.iterrows():
        mlbam = row.get("key_mlbam")
        bbref = row.get("key_bbref")
        if mlbam and isinstance(bbref, str) and bbref.strip():
            mapping[int(mlbam)] = bbref.strip()
    return mapping


def build_bbref_url(bbref_id: str) -> str:
    first_letter = bbref_id[0]
    return f"https://www.baseball-reference.com/players/{first_letter}/{bbref_id}.shtml"


def write_service_time_table(db_path: Path, cache: dict) -> int:
    rows = [
        (
            entry.get("mlbam_id"),
            entry.get("service_time_years"),
            entry.get("service_time_days"),
            entry.get("service_time_label"),
            entry.get("fetched_at"),
            DEFAULT_SOURCE,
        )
        for entry in cache.values()
        if entry.get("status") == "ok" and entry.get("mlbam_id") is not None
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS service_time_bref ("
            "mlbam_id INTEGER PRIMARY KEY, "
            "service_time_years INTEGER, "
            "service_time_days INTEGER, "
            "service_time_label TEXT, "
            "fetched_at TEXT, "
            "source TEXT)"
        )
        conn.executemany(
            "INSERT OR REPLACE INTO service_time_bref "
            "(mlbam_id, service_time_years, service_time_days, service_time_label, fetched_at, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Baseball-Reference service time for MLB players"
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to stats.db")
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.5,
        help="Seconds to sleep between Baseball-Reference requests",
    )
    parser.add_argument(
        "--max-players",
        type=int,
        default=None,
        help="Optional cap on number of players to fetch",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cache and refetch all",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and only fetch missing/zero service time rows",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=None,
        help="Cap total fetch attempts for this run (useful for batching)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print resume status and remaining IDs without making requests",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print service time coverage summary using local data (no web calls)",
    )
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Snapshot date (YYYY-MM-DD) used for summary ranking",
    )
    parser.add_argument(
        "--war-source",
        type=str,
        default="bWAR",
        help="WAR source for summary ranking",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Top-N size for summary ranking",
    )
    parser.add_argument(
        "--rank-by",
        choices=["tvp_p50", "tvp_risk_adj"],
        default=None,
        help="Ranking metric for summary (defaults to config)",
    )
    parser.add_argument(
        "--include-small-sample",
        action="store_true",
        help="Include small-sample players in summary leaderboard",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Refetch entries with status=error in the cache",
    )
    parser.add_argument(
        "--stop-on-rate-limit",
        action="store_true",
        default=True,
        help="Stop fetching when Baseball-Reference returns 429/403",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if args.resume:
        checkpoint = load_checkpoint(CHECKPOINT_FILE)
        if checkpoint:
            mlbam_ids = checkpoint.get("remaining_ids", [])
            start_index = int(checkpoint.get("index", 0))
        else:
            mlbam_ids = load_missing_service_ids(db_path)
            start_index = 0
            save_checkpoint(CHECKPOINT_FILE, mlbam_ids, start_index)
        print(f"Loaded {len(mlbam_ids)} missing MLBAM IDs for resume mode")
    else:
        mlbam_ids = load_mlbam_ids_from_db(db_path)
        if args.max_players:
            mlbam_ids = mlbam_ids[: args.max_players]
        start_index = 0
        print(f"Loaded {len(mlbam_ids)} MLBAM IDs from {db_path}")

    if args.status:
        remaining = len(mlbam_ids) - start_index
        print(f"Resume status: {remaining} remaining IDs (index={start_index})")
        return

    if args.summary:
        remaining_ids = mlbam_ids[start_index:]
        compute_service_time_summary(
            db_path=db_path,
            snapshot_date=args.snapshot_date,
            war_source=args.war_source,
            top_n=args.top,
            rank_by=args.rank_by,
            include_small_sample=args.include_small_sample,
            max_requests=args.max_requests,
            remaining_ids=remaining_ids,
            checkpoint_index=start_index,
        )
        return

    mlbam_to_bbref = map_to_bbref_ids(mlbam_ids)
    print(f"Mapped {len(mlbam_to_bbref)} MLBAM IDs to Baseball-Reference IDs")

    cache = load_cache()
    updated = 0

    rate_limit_hits = 0
    processed = 0
    idx = start_index
    while idx < len(mlbam_ids):
        mlbam_id = mlbam_ids[idx]
        bbref_id = mlbam_to_bbref.get(mlbam_id)
        if not bbref_id:
            idx += 1
            continue

        cache_key = bbref_id
        if not args.force_refresh and cache_key in cache:
            if not args.retry_errors:
                idx += 1
                continue
            if cache.get(cache_key, {}).get("status") != "error":
                idx += 1
                continue

        url = build_bbref_url(bbref_id)
        try:
            html = fetch_html(url)
            svc = parse_service_time(html)
            if svc is None:
                cache[cache_key] = {
                    "mlbam_id": mlbam_id,
                    "bbref_id": bbref_id,
                    "url": url,
                    "status": "not_found",
                    "fetched_at": datetime.utcnow().isoformat() + "Z",
                }
            else:
                cache[cache_key] = {
                    "mlbam_id": mlbam_id,
                    "bbref_id": bbref_id,
                    "url": url,
                    "status": "ok",
                    "service_time_label": svc.label,
                    "service_time_raw": svc.raw,
                    "service_time_years": svc.years,
                    "service_time_days": svc.days,
                    "fetched_at": datetime.utcnow().isoformat() + "Z",
                }
            updated += 1
        except HTTPError as exc:
            cache[cache_key] = {
                "mlbam_id": mlbam_id,
                "bbref_id": bbref_id,
                "url": url,
                "status": "error",
                "error": f"HTTP {exc.code}: {exc.reason}",
                "fetched_at": datetime.utcnow().isoformat() + "Z",
            }
            if exc.code in (403, 429):
                if args.stop_on_rate_limit and not args.resume:
                    print("Rate limited by Baseball-Reference; stopping early.")
                    break
                rate_limit_hits += 1
                backoff = min(60.0, args.sleep * (2 ** rate_limit_hits))
                print(f"Rate limited (HTTP {exc.code}); backing off for {backoff:.1f}s")
                time.sleep(backoff)
            else:
                rate_limit_hits = 0
        except Exception as exc:
            cache[cache_key] = {
                "mlbam_id": mlbam_id,
                "bbref_id": bbref_id,
                "url": url,
                "status": "error",
                "error": str(exc),
                "fetched_at": datetime.utcnow().isoformat() + "Z",
            }
            rate_limit_hits = 0
        finally:
            if args.sleep > 0:
                time.sleep(args.sleep)
        processed += 1
        idx += 1

        if args.resume:
            save_checkpoint(CHECKPOINT_FILE, mlbam_ids, idx)

        if idx % 50 == 0:
            print(f"Processed {idx} / {len(mlbam_ids)}")
            save_cache(cache)

        if args.max_requests is not None and processed >= args.max_requests:
            print("Reached max-requests limit; stopping early.")
            break

    if updated:
        save_cache(cache)

    # Build under-6 output
    under6 = []
    for entry in cache.values():
        if entry.get("status") != "ok":
            continue
        years = entry.get("service_time_years")
        if years is None:
            continue
        if int(years) < 6:
            under6.append(entry)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "baseball_reference",
            "db_path": str(db_path),
            "filter": "service_time_years < 6",
            "count": len(under6),
        },
        "players": sorted(
            under6,
            key=lambda x: (int(x.get("service_time_years", 0)), x.get("bbref_id", "")),
        ),
    }

    with OUTPUT_FILE.open("w") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {len(under6)} players to {OUTPUT_FILE}")
    inserted = write_service_time_table(db_path, cache)
    print(f"Upserted {inserted} service time rows into {db_path}")

    if args.resume and idx >= len(mlbam_ids):
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()


if __name__ == "__main__":
    main()
