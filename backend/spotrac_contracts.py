from __future__ import annotations

import json
import random
import re
import sqlite3
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(__file__).with_name("stats.db")
ID_MAP_PATH = REPO_ROOT / "data" / "mlb_api" / "id_map_mlbam_to_idfg.json"
OUTPUT_DIR = Path(__file__).with_name("output")
CACHE_DIR = Path(__file__).with_name("data") / "spotrac_cache"
TEAM_CACHE_DIR = CACHE_DIR / "teams"
PLAYER_CACHE_DIR = CACHE_DIR / "players"

SPOTRAC_BASE = "https://www.spotrac.com/mlb"
SNAPSHOT_DATE = "2025-11-01"
SNAPSHOT_SEASON = 2025
YEARS_REMAINING_BASE = 2026

BASE_DELAY_SECONDS = 1.2
DELAY_JITTER_SECONDS = 0.5
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

TEAM_SLUGS = {
    "arizona-diamondbacks": {"abbrev": "ARI", "name": "Arizona Diamondbacks"},
    "atlanta-braves": {"abbrev": "ATL", "name": "Atlanta Braves"},
    "baltimore-orioles": {"abbrev": "BAL", "name": "Baltimore Orioles"},
    "boston-red-sox": {"abbrev": "BOS", "name": "Boston Red Sox"},
    "chicago-cubs": {"abbrev": "CHC", "name": "Chicago Cubs"},
    "chicago-white-sox": {"abbrev": "CHW", "name": "Chicago White Sox"},
    "cincinnati-reds": {"abbrev": "CIN", "name": "Cincinnati Reds"},
    "cleveland-guardians": {"abbrev": "CLE", "name": "Cleveland Guardians"},
    "colorado-rockies": {"abbrev": "COL", "name": "Colorado Rockies"},
    "detroit-tigers": {"abbrev": "DET", "name": "Detroit Tigers"},
    "houston-astros": {"abbrev": "HOU", "name": "Houston Astros"},
    "kansas-city-royals": {"abbrev": "KCR", "name": "Kansas City Royals"},
    "los-angeles-angels": {"abbrev": "LAA", "name": "Los Angeles Angels"},
    "los-angeles-dodgers": {"abbrev": "LAD", "name": "Los Angeles Dodgers"},
    "miami-marlins": {"abbrev": "MIA", "name": "Miami Marlins"},
    "milwaukee-brewers": {"abbrev": "MIL", "name": "Milwaukee Brewers"},
    "minnesota-twins": {"abbrev": "MIN", "name": "Minnesota Twins"},
    "new-york-mets": {"abbrev": "NYM", "name": "New York Mets"},
    "new-york-yankees": {"abbrev": "NYY", "name": "New York Yankees"},
    "oakland-athletics": {"abbrev": "OAK", "name": "Oakland Athletics"},
    "philadelphia-phillies": {"abbrev": "PHI", "name": "Philadelphia Phillies"},
    "pittsburgh-pirates": {"abbrev": "PIT", "name": "Pittsburgh Pirates"},
    "san-diego-padres": {"abbrev": "SDP", "name": "San Diego Padres"},
    "san-francisco-giants": {"abbrev": "SFG", "name": "San Francisco Giants"},
    "seattle-mariners": {"abbrev": "SEA", "name": "Seattle Mariners"},
    "st-louis-cardinals": {"abbrev": "STL", "name": "St. Louis Cardinals"},
    "tampa-bay-rays": {"abbrev": "TBR", "name": "Tampa Bay Rays"},
    "texas-rangers": {"abbrev": "TEX", "name": "Texas Rangers"},
    "toronto-blue-jays": {"abbrev": "TOR", "name": "Toronto Blue Jays"},
    "washington-nationals": {"abbrev": "WSH", "name": "Washington Nationals"},
}

OPTION_TYPE_MAP = {
    "player": "PO",
    "club": "CO",
    "mutual": "MO",
}


@dataclass
class PlayerIndexEntry:
    player_id: int
    mlb_id: Optional[int]
    name: str
    team: str
    age: Optional[int]
    war_batting: Optional[float]
    war_pitching: Optional[float]

    @property
    def war_total(self) -> Optional[float]:
        values = [v for v in [self.war_batting, self.war_pitching] if v is not None]
        if not values:
            return None
        return round(sum(values), 3)


def normalize_name(name: str) -> str:
    name = re.sub(r"\(.*?\)", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-zA-Z\\s]", " ", name)
    name = re.sub(r"\\s+", " ", name).strip().lower()
    return name


def parse_money_to_m(value: str | None) -> Optional[float]:
    if not value:
        return None
    cleaned = value.replace("$", "").replace(",", "").replace("+", "").strip()
    if cleaned in {"-", ""}:
        return None

    multiplier = 1.0
    if cleaned.lower().endswith("m"):
        cleaned = cleaned[:-1]
        multiplier = 1.0
    elif cleaned.lower().endswith("k"):
        cleaned = cleaned[:-1]
        multiplier = 0.001

    try:
        number = float(cleaned)
    except ValueError:
        return None

    if number >= 1000:
        return round(number / 1_000_000, 3)
    return round(number * multiplier, 3)


def parse_year(value: str | None) -> Optional[int]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned.isdigit():
        return None
    return int(cleaned)


def fetch_url(url: str, cache_path: Path) -> tuple[str, str]:
    if cache_path.exists():
        html_text = cache_path.read_text(encoding="utf-8", errors="replace")
        scraped_at = datetime.utcfromtimestamp(cache_path.stat().st_mtime).isoformat()
        return html_text, scraped_at

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                html_text = response.read().decode("utf-8", errors="replace")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(html_text, encoding="utf-8")
            time.sleep(BASE_DELAY_SECONDS + random.uniform(0, DELAY_JITTER_SECONDS))
            return html_text, datetime.utcnow().isoformat()
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            last_error = exc
            time.sleep(2 + attempt)

    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def safe_cache_name(url: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", url.strip("/"))
    return safe[:200]


def parse_team_contracts(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
    header_map = {h.lower(): idx for idx, h in enumerate(headers)}

    def find_idx(names: list[str]) -> Optional[int]:
        for name in names:
            idx = header_map.get(name.lower())
            if idx is not None:
                return idx
        return None

    idx_player = 0
    idx_start = find_idx(["start", "start year"])
    idx_end = find_idx(["end"])
    idx_years = find_idx(["yrs", "years"])
    idx_value = find_idx(["value"])
    idx_aav = find_idx(["aav"])

    players = []
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        link = tds[idx_player].find("a") if len(tds) > idx_player else None
        if not link or not link.get("href"):
            continue
        name = link.get_text(" ", strip=True)
        player_url = link.get("href")
        players.append(
            {
                "player_name": name,
                "player_url": player_url,
                "start_year": parse_year(tds[idx_start].get_text(" ", strip=True))
                if idx_start is not None
                else None,
                "end_year": parse_year(tds[idx_end].get_text(" ", strip=True))
                if idx_end is not None
                else None,
                "contract_years": tds[idx_years].get_text(" ", strip=True)
                if idx_years is not None
                else None,
                "total_value_m": parse_money_to_m(
                    tds[idx_value].get_text(" ", strip=True)
                )
                if idx_value is not None
                else None,
                "aav_m": parse_money_to_m(tds[idx_aav].get_text(" ", strip=True))
                if idx_aav is not None
                else None,
            }
        )

    return players


def extract_contract_notes(soup: BeautifulSoup) -> list[str]:
    header = soup.find(lambda tag: tag.get_text(strip=True) == "Contract Notes")
    if not header:
        return []
    container = header.parent if header else None
    if not container:
        return []
    notes_list = container.find("ul")
    if not notes_list:
        notes_list = container.find_next("ul")
    if not notes_list:
        return []
    return [li.get_text(" ", strip=True) for li in notes_list.find_all("li")]


def extract_option_notes(notes: list[str]) -> dict[int, dict]:
    options: dict[int, dict] = {}
    option_re = re.compile(
        r"(?P<season>20\\d{2}).*(?P<type>Player|Club|Mutual) Option",
        re.IGNORECASE,
    )
    for note in notes:
        match = option_re.search(note)
        if not match:
            continue
        season = int(match.group("season"))
        option_type = OPTION_TYPE_MAP.get(match.group("type").lower())
        money_values = re.findall(r"\\$[\\d,.]+[MKmk]?", note)
        salary_m = parse_money_to_m(money_values[0]) if money_values else None
        buyout_m = None
        buyout_match = re.search(
            r"buyout[^$]*\\$[\\d,.]+[MKmk]?", note, re.IGNORECASE
        )
        if buyout_match:
            buyout_m = parse_money_to_m(
                re.search(r"\\$[\\d,.]+[MKmk]?", buyout_match.group(0)).group(0)
            )

        existing = options.get(season, {"season": season})
        if option_type:
            existing["type"] = option_type
        if salary_m is not None:
            existing["salary_m"] = salary_m
        if buyout_m is not None:
            existing["buyout_m"] = buyout_m
        options[season] = existing
    return options


def parse_contract_table(soup: BeautifulSoup) -> tuple[list[dict], dict[int, dict], Optional[int]]:
    contract_years: list[dict] = []
    options: dict[int, dict] = {}
    free_agent_year: Optional[int] = None

    table = None
    for candidate in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in candidate.find_all("th")]
        if headers and "Year" in headers and "Cash Total" in headers:
            table = candidate
            break
    if not table:
        return contract_years, options, free_agent_year

    headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
    header_map = {h: idx for idx, h in enumerate(headers)}
    idx_year = header_map.get("Year")
    idx_cash = header_map.get("Cash Total")
    idx_status = header_map.get("Status")

    rows = [row for row in table.find_all("tr") if row.find_all("td")]
    for row in rows:
        tds = row.find_all("td")
        if idx_year is None or idx_cash is None or idx_year >= len(tds):
            continue
        year_text = tds[idx_year].get_text(" ", strip=True)
        if not year_text.isdigit():
            continue
        season = int(year_text)
        salary_m = parse_money_to_m(tds[idx_cash].get_text(" ", strip=True))
        status = (
            tds[idx_status].get_text(" ", strip=True) if idx_status is not None else ""
        )
        status_lower = status.strip().lower()
        option_type = OPTION_TYPE_MAP.get(status_lower)
        if status_lower in {"ufa", "fa"}:
            free_agent_year = season if free_agent_year is None else free_agent_year
        if salary_m is None:
            continue

        is_guaranteed = option_type is None and "option" not in status_lower
        contract_years.append(
            {
                "season": season,
                "salary_m": salary_m,
                "is_guaranteed": is_guaranteed,
            }
        )

        if option_type:
            options[season] = {
                "season": season,
                "type": option_type,
                "salary_m": salary_m,
                "buyout_m": None,
            }

    return contract_years, options, free_agent_year


def parse_player_contract_page(html_text: str) -> tuple[list[dict], list[dict], Optional[int]]:
    soup = BeautifulSoup(html_text, "html.parser")
    contract_years, options_from_table, free_agent_year = parse_contract_table(soup)
    notes = extract_contract_notes(soup)
    options_from_notes = extract_option_notes(notes)

    options = {**options_from_table}
    for season, details in options_from_notes.items():
        existing = options.get(season, {"season": season})
        existing.update(details)
        options[season] = existing

    options_list = [options[key] for key in sorted(options)]
    return contract_years, options_list, free_agent_year


def load_player_index(season: int) -> dict[int, PlayerIndexEntry]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT player_id, name, team, age, war
        FROM batting_stats
        WHERE season = ?
        """,
        (season,),
    )
    batting_rows = cursor.fetchall()

    cursor.execute(
        """
        SELECT player_id, name, team, age, war
        FROM pitching_stats
        WHERE season = ?
        """,
        (season,),
    )
    pitching_rows = cursor.fetchall()
    conn.close()

    index: dict[int, PlayerIndexEntry] = {}

    for player_id, name, team, age, war in batting_rows:
        index[player_id] = PlayerIndexEntry(
            player_id=player_id,
            mlb_id=None,
            name=name,
            team=team,
            age=age,
            war_batting=war,
            war_pitching=None,
        )

    for player_id, name, team, age, war in pitching_rows:
        entry = index.get(player_id)
        if entry:
            entry.war_pitching = war
            if entry.age is None:
                entry.age = age
            if entry.team in {"", None}:
                entry.team = team
        else:
            index[player_id] = PlayerIndexEntry(
                player_id=player_id,
                mlb_id=None,
                name=name,
                team=team,
                age=age,
                war_batting=None,
                war_pitching=war,
            )

    return index


def apply_mlb_ids(index: dict[int, PlayerIndexEntry]) -> dict[int, int]:
    mlb_to_fg = json.loads(ID_MAP_PATH.read_text())
    fg_to_mlb = {
        int(fg_id): int(mlb_id)
        for mlb_id, fg_id in mlb_to_fg.items()
        if int(fg_id) > 0
    }

    for entry in index.values():
        entry.mlb_id = fg_to_mlb.get(entry.player_id)
    return fg_to_mlb


def build_matching_indexes(index: dict[int, PlayerIndexEntry]):
    by_team: dict[str, dict[str, PlayerIndexEntry]] = {}
    by_name: dict[str, list[PlayerIndexEntry]] = {}

    for entry in index.values():
        name_key = normalize_name(entry.name)
        by_team.setdefault(entry.team, {})[name_key] = entry
        by_name.setdefault(name_key, []).append(entry)

    return by_team, by_name


def fuzzy_match(
    name_key: str, team: str, by_team: dict[str, dict[str, PlayerIndexEntry]]
) -> Optional[PlayerIndexEntry]:
    candidates = list(by_team.get(team, {}).keys())
    if not candidates:
        return None
    close = [c for c in candidates if c.replace(" ", "") == name_key.replace(" ", "")]
    if close:
        return by_team[team][close[0]]
    return None


def match_player(
    player_name: str,
    team_abbrev: str,
    by_team: dict[str, dict[str, PlayerIndexEntry]],
    by_name: dict[str, list[PlayerIndexEntry]],
) -> tuple[Optional[PlayerIndexEntry], str]:
    name_key = normalize_name(player_name)
    team_map = by_team.get(team_abbrev, {})
    if name_key in team_map:
        return team_map[name_key], "team_exact"
    fuzzy = fuzzy_match(name_key, team_abbrev, by_team)
    if fuzzy:
        return fuzzy, "team_fuzzy"
    candidates = by_name.get(name_key, [])
    if len(candidates) == 1:
        return candidates[0], "name_only"
    if len(candidates) > 1:
        return None, "name_ambiguous"
    return None, "name_missing"


def compute_years_remaining(contract_years: list[dict]) -> tuple[int, int]:
    remaining = [
        entry for entry in contract_years if entry["season"] >= YEARS_REMAINING_BASE
    ]
    guaranteed = [entry for entry in remaining if entry.get("is_guaranteed")]
    return len(remaining), len(guaranteed)


def build_contract_outputs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEAM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PLAYER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    player_index = load_player_index(SNAPSHOT_SEASON)
    apply_mlb_ids(player_index)
    by_team, by_name = build_matching_indexes(player_index)

    contracts_by_mlb_id: dict[int, dict] = {}
    contracts_by_name_team: dict[tuple[str, str], dict] = {}
    unmatched_contracts: list[dict] = []

    for team_slug, team_info in TEAM_SLUGS.items():
        team_url = f"{SPOTRAC_BASE}/{team_slug}/contracts/"
        team_cache = TEAM_CACHE_DIR / f"{team_slug}.html"
        html_text, _ = fetch_url(team_url, team_cache)
        team_rows = parse_team_contracts(html_text)

        print(f"Spotrac: {team_info['name']} -> {len(team_rows)} players")
        for row in team_rows:
            player_url = row["player_url"]
            cache_name = safe_cache_name(player_url)
            player_cache = PLAYER_CACHE_DIR / f"{cache_name}.html"
            player_html, scraped_at = fetch_url(player_url, player_cache)
            contract_years, options, free_agent_year = parse_player_contract_page(
                player_html
            )

            years_remaining, guaranteed_remaining = compute_years_remaining(
                contract_years
            )

            contract = {
                "mlb_id": None,
                "player_name": row["player_name"],
                "contract_years": contract_years,
                "options": options,
                "aav_m": row["aav_m"],
                "total_value_m": row["total_value_m"],
                "free_agent_year": free_agent_year,
                "years_remaining": years_remaining,
                "guaranteed_years_remaining": guaranteed_remaining,
                "source_url": player_url,
                "last_scraped_at": scraped_at,
                "snapshot_date": SNAPSHOT_DATE,
            }

            entry, match_reason = match_player(
                row["player_name"], team_info["abbrev"], by_team, by_name
            )
            if entry and entry.mlb_id:
                contract["mlb_id"] = entry.mlb_id
                contract["player_name"] = entry.name
                existing = contracts_by_mlb_id.get(entry.mlb_id)
                if not existing or len(contract_years) > len(
                    existing.get("contract_years", [])
                ):
                    contracts_by_mlb_id[entry.mlb_id] = contract
            else:
                contracts_by_name_team[
                    (normalize_name(row["player_name"]), team_info["abbrev"].lower())
                ] = contract
                unmatched_contracts.append(
                    {
                        "player_name": row["player_name"],
                        "team": team_info["abbrev"],
                        "source_url": player_url,
                        "match_reason": match_reason,
                    }
                )

    contracts_payload = {
        "meta": {
            "snapshot_date": SNAPSHOT_DATE,
            "generated_at": datetime.utcnow().isoformat(),
            "season": SNAPSHOT_SEASON,
            "source": "spotrac",
        },
        "contracts": {str(k): v for k, v in contracts_by_mlb_id.items()},
        "unmatched_contracts": unmatched_contracts,
    }

    contracts_path = OUTPUT_DIR / "contracts_2025.json"
    contracts_path.write_text(json.dumps(contracts_payload, indent=2), encoding="utf-8")

    players_payload = {
        "meta": {
            "snapshot_date": SNAPSHOT_DATE,
            "generated_at": datetime.utcnow().isoformat(),
            "season": SNAPSHOT_SEASON,
            "source": "spotrac + fangraphs",
        },
        "players": [],
        "missing_contracts": [],
    }

    for entry in sorted(player_index.values(), key=lambda e: e.name):
        contract = None
        if entry.mlb_id and entry.mlb_id in contracts_by_mlb_id:
            contract = contracts_by_mlb_id[entry.mlb_id]
        else:
            fallback_key = (normalize_name(entry.name), entry.team.lower())
            contract = contracts_by_name_team.get(fallback_key)
        if not contract:
            players_payload["missing_contracts"].append(
                {
                    "mlb_id": entry.mlb_id,
                    "player_name": entry.name,
                    "team": entry.team,
                }
            )

        players_payload["players"].append(
            {
                "mlb_id": entry.mlb_id,
                "player_name": entry.name,
                "team": entry.team,
                "age": entry.age,
                "fwar": entry.war_total,
                "contract": contract,
            }
        )

    players_path = OUTPUT_DIR / "players_with_contracts_2025.json"
    players_path.write_text(json.dumps(players_payload, indent=2), encoding="utf-8")

    print(f"Wrote {contracts_path}")
    print(f"Wrote {players_path}")


if __name__ == "__main__":
    build_contract_outputs()
