import requests
from bs4 import BeautifulSoup, Comment
import json
import time
import os

# Configuration
BASE_URL = "https://www.baseball-reference.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
PROSPECTS_DIR = "backend/data/prospects_cache"
PROSPECTS_YEAR = 2026

NAME_KEYS = {"player", "name", "name_display", "player_name"}
RANK_KEYS = {"ranker", "rk", "rank"}
AGE_KEYS = {"age"}
POS_KEYS = {"pos", "position"}

def _extract_tables(soup):
    tables = list(soup.find_all("table"))
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if "<table" not in comment:
            continue
        comment_soup = BeautifulSoup(comment, "html.parser")
        tables.extend(comment_soup.find_all("table"))
    return tables

def _get_header_cells(table):
    thead = table.find("thead")
    header_rows = thead.find_all("tr") if thead else table.find_all("tr", limit=2)
    for row in reversed(header_rows):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2:
            return cells
    return []

def _normalize_header(cell):
    data_stat = (cell.get("data-stat") or "").strip().lower()
    if data_stat:
        return data_stat
    return cell.get_text(strip=True).lower()

def _find_header_index(headers, candidates):
    for key in candidates:
        if key in headers:
            return headers.index(key)
    return None

def scrape_team_prospects(team_name):
    """
    Scrapes the 2026 prospects page for a specific team.
    """
    # Format URL (e.g., New York Yankees -> new-york-yankees)
    url_safe_name = team_name.lower().replace(" ", "-")
    
    # Handle specific URL cases for teams with similar names
    # e.g., White Sox, Red Sox, Giants vs Dbacks etc usually follow specific patterns
    # B-Ref usually uses hyphens and full names except specific cases (e.g., White Sox).
    # Let's standardize:
    if "yankees" in url_safe_name: url_safe_name = "new-york-yankees"
    elif "mets" in url_safe_name: url_safe_name = "new-york-mets"
    elif "dodgers" in url_safe_name: url_safe_name = "los-angeles-dodgers"
    elif "angels" in url_safe_name: url_safe_name = "los-angeles-angels"
    elif "giants" in url_safe_name: url_safe_name = "san-francisco-giants"
    elif "athletics" in url_safe_name: url_safe_name = "oakland-athletics"
    elif "padres" in url_safe_name: url_safe_name = "san-diego-padres"
    elif "marlins" in url_safe_name: url_safe_name = "miami-marlins"
    elif "braves" in url_safe_name: url_safe_name = "atlanta-braves"
    elif "phillies" in url_safe_name: url_safe_name = "philadelphia-phillies"
    elif "nationals" in url_safe_name: url_safe_name = "washington-nationals"
    elif "cubs" in url_safe_name: url_safe_name = "chicago-cubs"
    elif "white sox" in url_safe_name: url_safe_name = "chicago-white-sox"
    elif "red sox" in url_safe_name: url_safe_name = "boston-red-sox"
    elif "tigers" in url_safe_name: url_safe_name = "detroit-tigers"
    elif "royals" in url_safe_name: url_safe_name = "kansas-city-royals"
    elif "twins" in url_safe_name: url_safe_name = "minnesota-twins"
    elif "indians" in url_safe_name or "guardians" in url_safe_name: url_safe_name = "cleveland-guardians"
    elif "brewers" in url_safe_name: url_safe_name = "milwaukee-brewers"
    elif "cardinals" in url_safe_name: url_safe_name = "st-louis-cardinals"
    elif "pirates" in url_safe_name: url_safe_name = "pittsburgh-pirates"
    elif "reds" in url_safe_name: url_safe_name = "cincinnati-reds"
    elif "rockies" in url_safe_name: url_safe_name = "colorado-rockies"
    elif "diamondbacks" in url_safe_name: url_safe_name = "arizona-diamondbacks"
    elif "rangers" in url_safe_name: url_safe_name = "texas-rangers"
    elif "astros" in url_safe_name: url_safe_name = "houston-astros"
    elif "blue jays" in url_safe_name: url_safe_name = "toronto-blue-jays"
    elif "rays" in url_safe_name: url_safe_name = "tampa-bay-rays"
    elif "orioles" in url_safe_name: url_safe_name = "baltimore-orioles"

    url = f"{BASE_URL}/teams/{url_safe_name}/{PROSPECTS_YEAR}-prospects/"
    
    # print(f"Fetching {team_name} from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"  Error: Status code {response.status_code} for {team_name} ({url})")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the main prospects table
        # The table usually has id 'stats' or class 'sortable'
        # We'll look for the table that has 'Age' in the header to be sure
        tables = _extract_tables(soup)
        
        target_table = None
        for table in tables:
            header_cells = _get_header_cells(table)
            if not header_cells:
                continue
            headers = [_normalize_header(cell) for cell in header_cells]
            if (any(key in headers for key in NAME_KEYS)
                and any(key in headers for key in AGE_KEYS)
                and any(key in headers for key in RANK_KEYS)):
                target_table = table
                break
        
        if not target_table:
            # Fallback
            target_table = soup.find('table', {'id': 'stats'})
            if not target_table:
                target_table = soup.find('table', class_='sortable stats_table')
        
        if not target_table:
            print(f"  Warning: No table found for {team_name}.")
            return []

        header_cells = _get_header_cells(target_table)
        headers = [_normalize_header(cell) for cell in header_cells]
        name_idx = _find_header_index(headers, NAME_KEYS)
        rank_idx = _find_header_index(headers, RANK_KEYS)
        age_idx = _find_header_index(headers, AGE_KEYS)
        pos_idx = _find_header_index(headers, POS_KEYS)

        players = []
        tbody = target_table.find("tbody")
        rows = tbody.find_all("tr") if tbody else target_table.find_all("tr")

        for row in rows:
            if "class" in row.attrs and "thead" in row["class"]:
                continue
            cols = row.find_all(['th', 'td'])
            if len(cols) < 2:
                continue
            
            # Extract Name (usually first td with data)
            name_cell = cols[name_idx] if name_idx is not None and name_idx < len(cols) else None
            name_link = name_cell.find('a') if name_cell else None
            if name_link:
                name = name_link.text.strip()
            elif name_cell:
                name = name_cell.text.strip()
            else:
                fallback_link = row.find('a')
                name = fallback_link.text.strip() if fallback_link else ""
            
            # Extract Rank (Rk) - usually 2nd column
            rank_cell = cols[rank_idx] if rank_idx is not None and rank_idx < len(cols) else None
            rank = rank_cell.text.strip() if rank_cell else "N/A"
            
            # Extract Age (usually 3rd column in B-Rank, Name, Age, ...)
            # Some pages have Rk, Name, Age
            # Let's try to find Age by iterating cols or guessing index
            age = "N/A"
            position = "N/A"
            
            # Heuristic parsing
            if age_idx is not None and age_idx < len(cols):
                age = cols[age_idx].text.strip() or "N/A"

            if pos_idx is not None and pos_idx < len(cols):
                position = cols[pos_idx].text.strip() or "N/A"

            if name:
                players.append({
                    "name": name,
                    "rank": rank,
                    "age": age,
                    "position": position,
                    "team": team_name
                })
        
        # print(f"  Found {len(players)} prospects.")
        return players

    except Exception as e:
        print(f"  Exception scraping {team_name}: {e}")
        return []

def save_prospects(players, team_name):
    """
    Saves a list of prospects to a JSON file.
    """
    if not players:
        return

    os.makedirs(PROSPECTS_DIR, exist_ok=True)
    filename = f"{PROSPECTS_DIR}/{team_name.lower().replace(' ', '')}_prospects.json"
    
    # Create a simple dictionary structure
    data = {
        "team": team_name,
        "count": len(players),
        "prospects": players,
        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    # print(f"  Saved to {filename}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        team = " ".join(sys.argv[1:])
        print(f"Manual scrape for: {team}")
        data = scrape_team_prospects(team)
        save_prospects(data, team)
    else:
        print("Usage: python prospects_scraper.py \"Team Name\"")
