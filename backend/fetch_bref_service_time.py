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
