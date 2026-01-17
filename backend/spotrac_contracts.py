from __future__ import annotations

import csv
import json
import math
import random
import re
import sqlite3
import time
import urllib.error
import urllib.request
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, Comment

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(__file__).with_name("stats.db")
ID_MAP_PATH = REPO_ROOT / "data" / "mlb_api" / "id_map_mlbam_to_idfg.json"
OUTPUT_DIR = Path(__file__).with_name("output")
CACHE_DIR = Path(__file__).with_name("data") / "spotrac_cache"
TEAM_CACHE_DIR = CACHE_DIR / "teams"
PLAYER_CACHE_DIR = CACHE_DIR / "players"
SPOTRAC_SEARCH_CACHE_DIR = CACHE_DIR / "search"
SPOTRAC_SEARCH_PLAYER_CACHE_DIR = CACHE_DIR / "search_players"
COTTS_CACHE_DIR = Path(__file__).with_name("data") / "cotts_cache"
COTTS_TEAM_CACHE_DIR = COTTS_CACHE_DIR / "teams"
COTTS_INDEX_CACHE = COTTS_CACHE_DIR / "cotts_index.html"
BREF_CACHE_DIR = Path(__file__).with_name("data") / "bref_cache"
BREF_PLAYER_CACHE_DIR = BREF_CACHE_DIR / "players"
BREF_REGISTER_CACHE = BREF_CACHE_DIR / "chadwick_register.csv"
MLB_API_CACHE_DIR = Path(__file__).with_name("data") / "mlb_api_cache"
MLB_API_PEOPLE_CACHE_DIR = MLB_API_CACHE_DIR / "people"
MLB_API_SEARCH_CACHE_DIR = MLB_API_CACHE_DIR / "people_search"

SPOTRAC_BASE = "https://www.spotrac.com/mlb"
COTTS_BASE = "https://legacy.baseballprospectus.com/compensation/cots"
BREF_BASE = "https://www.baseball-reference.com/players"
SNAPSHOT_DATE = "2025-11-01"
SNAPSHOT_SEASON = 2025
YEARS_REMAINING_BASE = 2026
MLB_MIN_SALARY_2025_M = 0.76

BASE_DELAY_SECONDS = 1.2
DELAY_JITTER_SECONDS = 0.5
MAX_RETRIES = 3
BREF_DELAY_SECONDS = 4.0
BREF_MAX_RETRIES = 6
SPOTRAC_SEARCH_DELAY_SECONDS = 2.0

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
    name = re.sub(r"[^a-zA-Z\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip().lower()
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


def parse_int_value(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def normalize_team_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name)
    return re.sub(r"\s+", " ", cleaned).strip().lower()


def normalize_short_year(value: str) -> Optional[int]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned.isdigit():
        return None
    year = int(cleaned)
    if year < 100:
        return 2000 + year
    if year < 1900 or year > 2100:
        return None
    return year


def parse_year_range(text: str) -> tuple[Optional[int], Optional[int]]:
    match = re.search(r"(\d{2,4})\s*-\s*(\d{2,4})", text)
    if not match:
        return None, None
    start = normalize_short_year(match.group(1))
    end = normalize_short_year(match.group(2))
    return start, end


def parse_contract_summary(summary: str) -> tuple[Optional[int], Optional[float], Optional[int], Optional[int], set[int]]:
    years_match = re.search(r"(\d+)\s*year", summary, re.IGNORECASE)
    years = int(years_match.group(1)) if years_match else None
    value_match = re.search(r"\$[\d,.]+[MKmk]?", summary)
    total_value_m = parse_money_to_m(value_match.group(0)) if value_match else None

    start_year = None
    end_year = None
    range_match = re.search(r"\((\d{4})(?:-(\d{2,4}))?\)", summary)
    if range_match:
        start_year = normalize_short_year(range_match.group(1))
        if range_match.group(2):
            end_year = normalize_short_year(range_match.group(2))
        else:
            end_year = start_year
    else:
        year_match = re.search(r"\b(20\d{2})\b", summary)
        if year_match:
            start_year = int(year_match.group(1))
            end_year = start_year

    option_years: set[int] = set()
    for match in re.finditer(r"(\d{4})\s+option", summary, re.IGNORECASE):
        option_years.add(int(match.group(1)))

    if years and start_year and end_year is None:
        end_year = start_year + years - 1

    return years, total_value_m, start_year, end_year, option_years


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


def fetch_bref_url(url: str, cache_path: Path) -> tuple[str, str]:
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
    for attempt in range(BREF_MAX_RETRIES):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                html_text = response.read().decode("utf-8", errors="replace")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(html_text, encoding="utf-8")
            time.sleep(BREF_DELAY_SECONDS + random.uniform(0, DELAY_JITTER_SECONDS))
            return html_text, datetime.utcnow().isoformat()
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code == 429:
                time.sleep(BREF_DELAY_SECONDS * (attempt + 2))
                continue
            time.sleep(2 + attempt)
        except urllib.error.URLError as exc:
            last_error = exc
            time.sleep(2 + attempt)

    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def fetch_spotrac_search_url(url: str, cache_path: Path) -> tuple[str, str]:
    if cache_path.exists():
        html_text = cache_path.read_text(encoding="utf-8", errors="replace")
        scraped_at = datetime.utcfromtimestamp(cache_path.stat().st_mtime).isoformat()
        return html_text, scraped_at

    try:
        from botasaurus.request import Request
    except ImportError as exc:
        raise RuntimeError("botasaurus is required for Spotrac search fallback") from exc

    req = Request()
    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            response = req.get(url)
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            html_text = response.text
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(html_text, encoding="utf-8")
            time.sleep(SPOTRAC_SEARCH_DELAY_SECONDS + random.uniform(0, DELAY_JITTER_SECONDS))
            return html_text, datetime.utcnow().isoformat()
        except Exception as exc:
            last_error = exc
            time.sleep(2 + attempt)

    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def extract_spotrac_player_url(html_text: str, name_key: str) -> Optional[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    meta = soup.find("meta", {"property": "og:url"})
    if meta and meta.get("content") and "/mlb/player/" in meta["content"]:
        return meta["content"]

    candidates = []
    for link in soup.select("a[href*=\"/mlb/player/\"]"):
        href = link.get("href")
        if not href:
            continue
        text = normalize_name(link.get_text(" ", strip=True))
        if not text:
            continue
        candidates.append((text, href))

    for text, href in candidates:
        if name_key and name_key in text:
            if href.startswith("/"):
                return f"https://www.spotrac.com{href}"
            return href

    if candidates:
        href = candidates[0][1]
        if href.startswith("/"):
            return f"https://www.spotrac.com{href}"
        return href

    return None


def fetch_mlb_api_json(url: str, cache_path: Path) -> dict:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload), encoding="utf-8")
    time.sleep(BASE_DELAY_SECONDS + random.uniform(0, DELAY_JITTER_SECONDS))
    return payload


def resolve_mlb_id_from_search(name: str) -> Optional[int]:
    query = urllib.parse.quote_plus(name)
    cache_name = safe_cache_name(name)
    url = f"https://statsapi.mlb.com/api/v1/people/search?names={query}"
    cache_path = MLB_API_SEARCH_CACHE_DIR / f"{cache_name}.json"
    data = fetch_mlb_api_json(url, cache_path)
    people = data.get("people") or []
    if not people:
        return None

    name_key = normalize_name(name)
    exact = [
        person for person in people if normalize_name(person.get("fullName", "")) == name_key
    ]
    if len(exact) == 1:
        return parse_int_value(exact[0].get("id"))
    if len(people) == 1:
        return parse_int_value(people[0].get("id"))
    return None


def resolve_mlb_id_from_pybaseball(name: str) -> Optional[int]:
    parts = name.split()
    if len(parts) < 2:
        return None
    first = parts[0].lower()
    last = parts[-1].lower()
    first_variants = [first]
    nicknames = {
        "cameron": "cam",
        "jackson": "jack",
    }
    if first in nicknames:
        first_variants.append(nicknames[first])
    if len(first) > 3:
        first_variants.append(first[:3])

    try:
        from pybaseball import playerid_lookup
    except ImportError:
        return None

    for variant in first_variants:
        df = playerid_lookup(last, variant)
        if df.empty:
            continue
        df = df.copy()
        df["mlb_played_last"] = df["mlb_played_last"].fillna(0)
        df = df.sort_values("mlb_played_last", ascending=False)
        mlb_id = parse_int_value(df.iloc[0].get("key_mlbam"))
        if mlb_id:
            return mlb_id
    return None


def fetch_mlb_person(mlb_id: int) -> Optional[dict]:
    cache_path = MLB_API_PEOPLE_CACHE_DIR / f"{mlb_id}.json"
    url = f"https://statsapi.mlb.com/api/v1/people/{mlb_id}"
    data = fetch_mlb_api_json(url, cache_path)
    people = data.get("people") or []
    if not people:
        return None
    return people[0]


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
        r"(?P<season>20\d{2}).*(?P<type>Player|Club|Mutual) Option",
        re.IGNORECASE,
    )
    for note in notes:
        match = option_re.search(note)
        if not match:
            continue
        season = int(match.group("season"))
        option_type = OPTION_TYPE_MAP.get(match.group("type").lower())
        money_values = re.findall(r"\$[\d,.]+[MKmk]?", note)
        salary_m = parse_money_to_m(money_values[0]) if money_values else None
        buyout_m = None
        buyout_match = re.search(
            r"buyout[^$]*\$[\d,.]+[MKmk]?", note, re.IGNORECASE
        )
        if buyout_match:
            buyout_m = parse_money_to_m(
                re.search(r"\$[\d,.]+[MKmk]?", buyout_match.group(0)).group(0)
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


def extract_cotts_team_urls() -> dict[str, str]:
    html_text, _ = fetch_url(COTTS_BASE, COTTS_INDEX_CACHE)
    soup = BeautifulSoup(html_text, "html.parser")
    team_urls: dict[str, str] = {}
    name_to_abbrev = {
        normalize_team_name(info["name"]): info["abbrev"] for info in TEAM_SLUGS.values()
    }

    for link in soup.find_all("a"):
        href = link.get("href")
        text = link.get_text(" ", strip=True)
        if not href or not text:
            continue
        normalized = normalize_team_name(text)
        abbrev = name_to_abbrev.get(normalized)
        if not abbrev:
            continue
        if abbrev not in team_urls:
            team_urls[abbrev] = href

    return team_urls


def parse_cotts_details(details: list[str]) -> tuple[dict[int, float], dict[int, str], dict[int, float], set[int]]:
    salary_by_year: dict[int, float] = {}
    option_types: dict[int, str] = {}
    buyouts: dict[int, float] = {}
    option_years: set[int] = set()

    for raw_line in details:
        line = " ".join(raw_line.split())
        if not line:
            continue

        range_match = re.search(
            r"(\d{2,4})\s*[-–]\s*(\d{2,4}).*?\$([\d,.]+[MKmk]?)",
            line,
        )
        if range_match and re.search(r"annual|per year", line, re.IGNORECASE):
            start = normalize_short_year(range_match.group(1))
            end = normalize_short_year(range_match.group(2))
            salary_m = parse_money_to_m(f"${range_match.group(3)}")
            if start and end and salary_m is not None:
                for year in range(start, end + 1):
                    salary_by_year.setdefault(year, salary_m)

        for match in re.finditer(r"(\d{2,4})[^$]{0,40}\$([\d,.]+[MKmk]?)", line):
            year = normalize_short_year(match.group(1))
            salary_m = parse_money_to_m(f"${match.group(2)}")
            if year and salary_m is not None:
                salary_by_year.setdefault(year, salary_m)

        option_found = False
        range_option = re.search(
            r"(\d{2,4})\s*[-–]\s*(\d{2,4})\s+(player|club|mutual) option",
            line,
            re.IGNORECASE,
        )
        if range_option:
            start = normalize_short_year(range_option.group(1))
            end = normalize_short_year(range_option.group(2))
            option_type = OPTION_TYPE_MAP.get(range_option.group(3).lower())
            if start and end and option_type:
                for year in range(start, end + 1):
                    option_types[year] = option_type
                    option_years.add(year)
                option_found = True

        if not option_found:
            single_option = re.search(
                r"(\d{2,4})\s*[: ]\s*\$[^,]*?(player|club|mutual) option",
                line,
                re.IGNORECASE,
            )
            if not single_option:
                single_option = re.search(
                    r"(\d{2,4})\s+(player|club|mutual) option",
                    line,
                    re.IGNORECASE,
                )
            if single_option:
                year = normalize_short_year(single_option.group(1))
                option_type = OPTION_TYPE_MAP.get(single_option.group(2).lower())
                if year and option_type:
                    option_types[year] = option_type
                    option_years.add(year)
                option_found = True

        if "option" in line.lower() and not option_found:
            loose_match = re.search(r"(20\d{2})\s+option", line, re.IGNORECASE)
            if loose_match:
                year = normalize_short_year(loose_match.group(1))
                if year:
                    option_years.add(year)

        if "buyout" in line.lower():
            buyout_match = re.search(r"buyout[^$]*\$([\d,.]+[MKmk]?)", line, re.IGNORECASE)
            if buyout_match:
                buyout_m = parse_money_to_m(f"${buyout_match.group(1)}")
                if buyout_m is not None:
                    year_match = re.search(r"(\d{2,4})", line)
                    year = normalize_short_year(year_match.group(1)) if year_match else None
                    if year:
                        buyouts[year] = buyout_m

    return salary_by_year, option_types, buyouts, option_years


def parse_cotts_contract(summary: str, details: list[str]) -> tuple[list[dict], list[dict], Optional[float], Optional[float], Optional[int]]:
    years, total_value_m, start_year, end_year, option_years_from_summary = (
        parse_contract_summary(summary)
    )
    salary_by_year, option_types, buyouts, option_years_from_details = parse_cotts_details(details)

    option_years = set(option_years_from_summary)

    if start_year and end_year:
        option_years.update(
            year
            for year in option_years_from_details
            if start_year <= year <= end_year
        )
        allowed_years = set(range(start_year, end_year + 1)) | option_years
        salary_by_year = {
            year: salary for year, salary in salary_by_year.items() if year in allowed_years
        }
        option_types = {
            year: option_type for year, option_type in option_types.items() if year in allowed_years
        }
        buyouts = {year: buyout for year, buyout in buyouts.items() if year in allowed_years}

    aav_m = None
    if years and total_value_m is not None and years > 0:
        aav_m = round(total_value_m / years, 3)

    contract_years: list[dict] = []
    if salary_by_year:
        for year in sorted(salary_by_year):
            contract_years.append(
                {
                    "season": year,
                    "salary_m": salary_by_year[year],
                    "is_guaranteed": year not in option_years,
                }
            )
    elif start_year:
        end_year = end_year or start_year
        per_year_m = aav_m if aav_m is not None else total_value_m
        for year in range(start_year, end_year + 1):
            contract_years.append(
                {
                    "season": year,
                    "salary_m": per_year_m,
                    "is_guaranteed": year not in option_years,
                }
            )

    if total_value_m is None and contract_years:
        values = [entry["salary_m"] for entry in contract_years if entry["salary_m"]]
        if values:
            total_value_m = round(sum(values), 3)
    if aav_m is None and contract_years:
        values = [entry["salary_m"] for entry in contract_years if entry["salary_m"]]
        if values:
            aav_m = round(sum(values) / len(values), 3)

    options_list = []
    for year, option_type in option_types.items():
        options_list.append(
            {
                "season": year,
                "type": option_type,
                "salary_m": salary_by_year.get(year),
                "buyout_m": buyouts.get(year),
            }
        )

    return contract_years, options_list, aav_m, total_value_m, None


def parse_cotts_team_players(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    content = soup.find("div", class_="entry-content") or soup
    players: list[dict] = []

    for p in content.find_all("p"):
        name_span = p.find(
            "span", style=lambda s: s and "font-size: 130%" in s
        )
        if not name_span:
            continue
        name = name_span.get_text(" ", strip=True)
        text = p.get_text(" ", strip=True)
        if name not in text:
            continue

        remainder = text.replace(name, "", 1).strip()
        if not remainder:
            continue

        parts = remainder.split()
        summary_start = None
        for idx, token in enumerate(parts):
            lower = token.lower()
            if "$" in token or re.search(r"\d{4}", token):
                summary_start = idx
                break
            if token.isdigit() and idx + 1 < len(parts):
                next_token = parts[idx + 1].lower()
                if next_token.startswith("year"):
                    summary_start = idx
                    break
            if lower.startswith("year"):
                summary_start = idx
                break

        if summary_start is None:
            continue

        position = " ".join(parts[:summary_start]).strip()
        summary = " ".join(parts[summary_start:]).strip()

        if not summary or not re.search(r"\$|20\d{2}", summary):
            continue
        if not re.search(
            r"\b(rhp|lhp|p|c|1b|2b|3b|ss|of|lf|cf|rf|dh|if|ut)\b",
            position,
            re.IGNORECASE,
        ):
            continue

        details: list[str] = []
        sibling = p.find_next_sibling()
        if sibling and sibling.name == "ul":
            details = [li.get_text(" ", strip=True) for li in sibling.find_all("li")]

        players.append(
            {
                "player_name": name,
                "position": position,
                "summary": summary,
                "details": details,
            }
        )

    return players


def load_chadwick_register_rows() -> list[dict]:
    if BREF_REGISTER_CACHE.exists():
        rows: list[dict] = []
        with BREF_REGISTER_CACHE.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(row)
        return rows

    from pybaseball import chadwick_register

    df = chadwick_register()
    columns = [
        "name_last",
        "name_first",
        "key_mlbam",
        "key_bbref",
        "key_fangraphs",
        "mlb_played_last",
    ]
    df = df[columns]
    BREF_REGISTER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(BREF_REGISTER_CACHE, index=False)
    return df.to_dict(orient="records")


def build_chadwick_maps() -> tuple[dict[int, int], dict[int, str], dict[str, list[dict]]]:
    rows = load_chadwick_register_rows()
    fg_to_mlb: dict[int, int] = {}
    mlb_to_bbref: dict[int, str] = {}
    name_to_rows: dict[str, list[dict]] = {}

    for row in rows:
        mlb_id = parse_int_value(row.get("key_mlbam"))
        fg_id = parse_int_value(row.get("key_fangraphs"))
        bbref_id = row.get("key_bbref") or None
        last_played = parse_int_value(row.get("mlb_played_last"))

        if fg_id and mlb_id and fg_id not in fg_to_mlb:
            fg_to_mlb[fg_id] = mlb_id
        if mlb_id and bbref_id:
            mlb_to_bbref[mlb_id] = bbref_id

        name_key = normalize_name(
            f"{row.get('name_first', '')} {row.get('name_last', '')}"
        )
        if not name_key:
            continue
        name_to_rows.setdefault(name_key, []).append(
            {
                "mlb_id": mlb_id,
                "bbref_id": bbref_id,
                "mlb_played_last": last_played,
            }
        )

    return fg_to_mlb, mlb_to_bbref, name_to_rows


def select_chadwick_candidate(candidates: list[dict]) -> tuple[Optional[dict], str]:
    if not candidates:
        return None, "name_missing"

    def score(row: dict) -> tuple[int, int]:
        last_played = row.get("mlb_played_last") or 0
        recent_flag = 1 if last_played >= SNAPSHOT_SEASON - 1 else 0
        return recent_flag, last_played

    sorted_candidates = sorted(candidates, key=score, reverse=True)
    best = sorted_candidates[0]
    if len(sorted_candidates) > 1 and score(sorted_candidates[0]) == score(
        sorted_candidates[1]
    ):
        return best, "name_ambiguous"
    return best, "name_match"


def bref_player_url(bbref_id: str) -> str:
    return f"{BREF_BASE}/{bbref_id[0]}/{bbref_id}.shtml"


def parse_bref_salary_amount(cell: Optional[BeautifulSoup]) -> Optional[float]:
    if not cell:
        return None
    amount = cell.get("data-amount")
    if amount:
        try:
            return round(float(amount) / 1_000_000, 3)
        except ValueError:
            pass
    text = cell.get_text(" ", strip=True)
    return parse_money_to_m(text)


def parse_bref_salaries(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    table_html = None
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if 'id="br-salaries"' in comment:
            table_html = comment
            break
    if not table_html:
        return []

    comment_soup = BeautifulSoup(table_html, "html.parser")
    table = comment_soup.find("table", {"id": "br-salaries"})
    if not table:
        return []

    contract_years: list[dict] = []
    for row in table.select("tbody tr"):
        year_cell = row.find("th", {"data-stat": "year_ID"})
        year = parse_year(year_cell.get_text(" ", strip=True)) if year_cell else None
        if not year:
            continue
        salary_cell = row.find(
            "td", attrs={"data-stat": re.compile(r"^salary$", re.IGNORECASE)}
        )
        salary_m = parse_bref_salary_amount(salary_cell)
        if salary_m is None:
            continue
        contract_years.append(
            {
                "season": year,
                "salary_m": salary_m,
                "is_guaranteed": True,
            }
        )

    return contract_years


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


def apply_mlb_ids(
    index: dict[int, PlayerIndexEntry],
    chadwick_fangraphs: dict[int, int],
    chadwick_names: dict[str, list[dict]],
) -> list[dict]:
    mlb_to_fg = json.loads(ID_MAP_PATH.read_text())
    fg_to_mlb = {
        int(fg_id): int(mlb_id)
        for mlb_id, fg_id in mlb_to_fg.items()
        if int(fg_id) > 0
    }

    for fg_id, mlb_id in chadwick_fangraphs.items():
        fg_to_mlb.setdefault(fg_id, mlb_id)

    warnings: list[dict] = []
    for entry in index.values():
        entry.mlb_id = fg_to_mlb.get(entry.player_id)
        if entry.mlb_id:
            continue
        candidates = chadwick_names.get(normalize_name(entry.name), [])
        best, reason = select_chadwick_candidate(candidates)
        if best and best.get("mlb_id"):
            entry.mlb_id = best["mlb_id"]
            if reason == "name_ambiguous":
                warnings.append(
                    {
                        "player_name": entry.name,
                        "team": entry.team,
                        "match_reason": reason,
                    }
                )
    return warnings


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
    COTTS_TEAM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    BREF_PLAYER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SPOTRAC_SEARCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SPOTRAC_SEARCH_PLAYER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MLB_API_PEOPLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MLB_API_SEARCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    player_index = load_player_index(SNAPSHOT_SEASON)
    chadwick_fangraphs, mlb_to_bbref, chadwick_names = build_chadwick_maps()
    id_match_warnings = apply_mlb_ids(
        player_index, chadwick_fangraphs, chadwick_names
    )
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
                        "source": "spotrac",
                    }
                )

    cotts_team_urls = extract_cotts_team_urls()
    for team_abbrev, team_url in cotts_team_urls.items():
        team_cache = COTTS_TEAM_CACHE_DIR / f"{team_abbrev.lower()}.html"
        html_text, scraped_at = fetch_url(team_url, team_cache)
        cotts_players = parse_cotts_team_players(html_text)
        print(f"Cotts: {team_abbrev} -> {len(cotts_players)} players")

        for player in cotts_players:
            contract_years, options, aav_m, total_value_m, free_agent_year = (
                parse_cotts_contract(player["summary"], player["details"])
            )
            if not contract_years and not options and total_value_m is None:
                continue

            years_remaining, guaranteed_remaining = compute_years_remaining(
                contract_years
            )

            contract = {
                "mlb_id": None,
                "player_name": player["player_name"],
                "contract_years": contract_years,
                "options": options,
                "aav_m": aav_m,
                "total_value_m": total_value_m,
                "free_agent_year": free_agent_year,
                "years_remaining": years_remaining,
                "guaranteed_years_remaining": guaranteed_remaining,
                "source_url": team_url,
                "last_scraped_at": scraped_at,
                "snapshot_date": SNAPSHOT_DATE,
            }

            entry, match_reason = match_player(
                player["player_name"], team_abbrev, by_team, by_name
            )
            if entry and entry.mlb_id:
                contract["mlb_id"] = entry.mlb_id
                contract["player_name"] = entry.name
                if entry.mlb_id not in contracts_by_mlb_id:
                    contracts_by_mlb_id[entry.mlb_id] = contract
            else:
                contracts_by_name_team[
                    (normalize_name(player["player_name"]), team_abbrev.lower())
                ] = contract
                unmatched_contracts.append(
                    {
                        "player_name": player["player_name"],
                        "team": team_abbrev,
                        "source_url": team_url,
                        "match_reason": match_reason,
                        "source": "cotts",
                    }
                )

    missing_entries: list[PlayerIndexEntry] = []
    for entry in player_index.values():
        if entry.mlb_id and entry.mlb_id in contracts_by_mlb_id:
            continue
        fallback_key = (normalize_name(entry.name), entry.team.lower())
        if fallback_key in contracts_by_name_team:
            continue
        missing_entries.append(entry)

    bref_added = 0
    bref_fetches = 0
    bref_cache_hits = 0
    bref_reused = 0
    bref_contracts: dict[str, dict] = {}
    total_missing = len(missing_entries)
    if total_missing:
        print(f"BRef: processing {total_missing} missing players")
    for idx, entry in enumerate(missing_entries, start=1):
        bbref_id = None
        if entry.mlb_id:
            bbref_id = mlb_to_bbref.get(entry.mlb_id)
        if not bbref_id:
            candidates = chadwick_names.get(normalize_name(entry.name), [])
            best, _ = select_chadwick_candidate(candidates)
            if best:
                bbref_id = best.get("bbref_id")
                if entry.mlb_id is None and best.get("mlb_id"):
                    entry.mlb_id = best["mlb_id"]
        if not bbref_id:
            continue
        if bbref_id in bref_contracts:
            contract_template = bref_contracts[bbref_id]
            contract = json.loads(json.dumps(contract_template))
            contract["mlb_id"] = entry.mlb_id
            contract["player_name"] = entry.name
            if entry.mlb_id:
                contracts_by_mlb_id.setdefault(entry.mlb_id, contract)
                bref_added += 1
            contracts_by_name_team[
                (normalize_name(entry.name), entry.team.lower())
            ] = contract
            bref_reused += 1
            continue

        player_url = bref_player_url(bbref_id)
        player_cache = BREF_PLAYER_CACHE_DIR / f"{bbref_id}.html"
        cache_hit = player_cache.exists()
        try:
            player_html, scraped_at = fetch_bref_url(player_url, player_cache)
        except RuntimeError as exc:
            print(f"BRef: failed {bbref_id}: {exc}")
            continue
        if cache_hit:
            bref_cache_hits += 1
        else:
            bref_fetches += 1
        contract_years = parse_bref_salaries(player_html)
        if not contract_years:
            continue
        contract_years = sorted(contract_years, key=lambda row: row["season"])
        filtered_years = [
            row for row in contract_years if row["season"] >= SNAPSHOT_SEASON
        ]
        if filtered_years:
            contract_years = filtered_years
        else:
            contract_years = [contract_years[-1]]

        total_value_m = sum(
            entry_year["salary_m"]
            for entry_year in contract_years
            if entry_year.get("salary_m") is not None
        )
        aav_m = (
            round(total_value_m / len(contract_years), 3)
            if contract_years
            else None
        )
        years_remaining, guaranteed_remaining = compute_years_remaining(contract_years)

        contract_template = {
            "mlb_id": None,
            "player_name": None,
            "contract_years": contract_years,
            "options": [],
            "aav_m": aav_m,
            "total_value_m": round(total_value_m, 3) if contract_years else None,
            "free_agent_year": None,
            "years_remaining": years_remaining,
            "guaranteed_years_remaining": guaranteed_remaining,
            "source_url": player_url,
            "last_scraped_at": scraped_at,
            "snapshot_date": SNAPSHOT_DATE,
        }
        bref_contracts[bbref_id] = contract_template
        contract = json.loads(json.dumps(contract_template))
        contract["mlb_id"] = entry.mlb_id
        contract["player_name"] = entry.name

        if entry.mlb_id:
            contracts_by_mlb_id.setdefault(entry.mlb_id, contract)
            bref_added += 1
        contracts_by_name_team[
            (normalize_name(entry.name), entry.team.lower())
        ] = contract
        if idx == 1 or idx % 25 == 0 or idx == total_missing:
            print(
                "BRef: "
                f"{idx}/{total_missing} processed "
                f"(added {bref_added}, fetched {bref_fetches}, "
                f"cached {bref_cache_hits}, reused {bref_reused})"
            )

    if id_match_warnings:
        print(f"Chadwick name matches ambiguous: {len(id_match_warnings)}")
    if bref_added:
        print(f"BRef: added {bref_added} contracts")

    remaining_entries: list[PlayerIndexEntry] = []
    for entry in missing_entries:
        if entry.mlb_id and entry.mlb_id in contracts_by_mlb_id:
            continue
        fallback_key = (normalize_name(entry.name), entry.team.lower())
        if fallback_key in contracts_by_name_team:
            continue
        remaining_entries.append(entry)

    spotrac_search_added = 0
    if remaining_entries:
        print(f"Spotrac search: processing {len(remaining_entries)} players")
    for idx, entry in enumerate(remaining_entries, start=1):
        name_key = normalize_name(entry.name)
        if not name_key:
            continue
        search_query = urllib.parse.quote_plus(entry.name)
        search_url = f"https://www.spotrac.com/search/?q={search_query}"
        search_cache = SPOTRAC_SEARCH_CACHE_DIR / f"{name_key}.html"
        try:
            search_html, _ = fetch_spotrac_search_url(search_url, search_cache)
        except RuntimeError as exc:
            print(f"Spotrac search: failed {entry.name}: {exc}")
            continue

        player_url = extract_spotrac_player_url(search_html, name_key)
        player_html = None
        scraped_at = None
        if player_url:
            cache_name = safe_cache_name(player_url)
            player_cache = SPOTRAC_SEARCH_PLAYER_CACHE_DIR / f"{cache_name}.html"
            try:
                player_html, scraped_at = fetch_spotrac_search_url(
                    player_url, player_cache
                )
            except RuntimeError as exc:
                print(f"Spotrac search: failed {player_url}: {exc}")
                continue
        else:
            player_html = search_html
            scraped_at = datetime.utcnow().isoformat()

        contract_years, options, free_agent_year = parse_player_contract_page(
            player_html
        )
        if not contract_years:
            continue

        values = [
            entry_year["salary_m"]
            for entry_year in contract_years
            if entry_year.get("salary_m") is not None
        ]
        total_value_m = round(sum(values), 3) if values else None
        aav_m = round(total_value_m / len(values), 3) if values else None
        years_remaining, guaranteed_remaining = compute_years_remaining(contract_years)

        contract = {
            "mlb_id": entry.mlb_id,
            "player_name": entry.name,
            "contract_years": contract_years,
            "options": options,
            "aav_m": aav_m,
            "total_value_m": total_value_m,
            "free_agent_year": free_agent_year,
            "years_remaining": years_remaining,
            "guaranteed_years_remaining": guaranteed_remaining,
            "source_url": player_url or search_url,
            "last_scraped_at": scraped_at,
            "snapshot_date": SNAPSHOT_DATE,
        }

        if entry.mlb_id:
            contracts_by_mlb_id.setdefault(entry.mlb_id, contract)
            spotrac_search_added += 1
        contracts_by_name_team[
            (normalize_name(entry.name), entry.team.lower())
        ] = contract

        if idx == 1 or idx % 25 == 0 or idx == len(remaining_entries):
            print(
                "Spotrac search: "
                f"{idx}/{len(remaining_entries)} processed "
                f"(added {spotrac_search_added})"
            )

    derived_added = 0
    final_missing: list[PlayerIndexEntry] = []
    for entry in remaining_entries:
        if entry.mlb_id and entry.mlb_id in contracts_by_mlb_id:
            continue
        fallback_key = (normalize_name(entry.name), entry.team.lower())
        if fallback_key in contracts_by_name_team:
            continue
        final_missing.append(entry)

    if final_missing:
        print(f"Derived minimum: processing {len(final_missing)} players")
    for entry in final_missing:
        if entry.mlb_id is None:
            entry.mlb_id = resolve_mlb_id_from_search(entry.name)
        if entry.mlb_id is None:
            entry.mlb_id = resolve_mlb_id_from_pybaseball(entry.name)

        if entry.mlb_id is None:
            continue
        person = fetch_mlb_person(entry.mlb_id)
        debut_date = (person or {}).get("mlbDebutDate")
        if not debut_date:
            continue
        if debut_date > SNAPSHOT_DATE:
            continue

        contract_years = [
            {
                "season": SNAPSHOT_SEASON,
                "salary_m": MLB_MIN_SALARY_2025_M,
                "is_guaranteed": True,
            }
        ]
        years_remaining, guaranteed_remaining = compute_years_remaining(contract_years)
        contract = {
            "mlb_id": entry.mlb_id,
            "player_name": entry.name,
            "contract_years": contract_years,
            "options": [],
            "aav_m": MLB_MIN_SALARY_2025_M,
            "total_value_m": MLB_MIN_SALARY_2025_M,
            "free_agent_year": None,
            "years_remaining": years_remaining,
            "guaranteed_years_remaining": guaranteed_remaining,
            "source_url": "derived:mlb-minimum",
            "last_scraped_at": datetime.utcnow().isoformat(),
            "snapshot_date": SNAPSHOT_DATE,
        }

        contracts_by_mlb_id.setdefault(entry.mlb_id, contract)
        contracts_by_name_team[
            (normalize_name(entry.name), entry.team.lower())
        ] = contract
        derived_added += 1

    if derived_added:
        print(f"Derived minimum: added {derived_added} contracts")

    contracts_payload = {
        "meta": {
            "snapshot_date": SNAPSHOT_DATE,
            "generated_at": datetime.utcnow().isoformat(),
            "season": SNAPSHOT_SEASON,
            "source": "spotrac + cotts + bref + spotrac-search + derived-min",
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
            "source": "spotrac + cotts + bref + spotrac-search + derived-min + fangraphs",
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
