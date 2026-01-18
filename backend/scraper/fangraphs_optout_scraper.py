#!/usr/bin/env python3
"""
Fangraphs RosterResource Opt-Out Scraper
Scrapes opt-out clauses from Fangraphs payroll pages using Botasaurus

Usage:
    python backend/scraper/fangraphs_optout_scraper.py
"""

from botasaurus.browser_decorator import browser
from botasaurus_driver import Driver
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime
import os

# Type mappings for opt-out clauses
TYPE_MAPPING = {
    "player opt-out": "PO",
    "player optout": "PO",
    "club opt-out": "CO",
    "team opt-out": "CO",
    "mutual opt-out": "MO"
}

# Team mappings (Fangraphs URL format)
TEAMS = {
    "Los Angeles Angels": "angels",
    "Houston Astros": "astros",
    "Oakland Athletics": "athletics",
    "Toronto Blue Jays": "bluejays",
    "Atlanta Braves": "braves",
    "Milwaukee Brewers": "brewers",
    "St. Louis Cardinals": "cardinals",
    "Chicago Cubs": "cubs",
    "Arizona Diamondbacks": "dbacks",
    "Los Angeles Dodgers": "dodgers",
    "San Francisco Giants": "giants",
    "Cleveland Guardians": "guardians",
    "Seattle Mariners": "mariners",
    "Miami Marlins": "marlins",
    "New York Mets": "mets",
    "Washington Nationals": "nationals",
    "Baltimore Orioles": "orioles",
    "San Diego Padres": "padres",
    "Philadelphia Phillies": "phillies",
    "Pittsburgh Pirates": "pirates",
    "Texas Rangers": "rangers",
    "Tampa Bay Rays": "rays",
    "Cincinnati Reds": "reds",
    "Boston Red Sox": "redsox",
    "Colorado Rockies": "rockies",
    "Kansas City Royals": "royals",
    "Detroit Tigers": "tigers",
    "Minnesota Twins": "twins",
    "Chicago White Sox": "whitesox",
    "New York Yankees": "yankees"
}

# Abbreviation mappings for output
ABBREV_MAP = {
    "angels": "LAA",
    "astros": "HOU",
    "athletics": "OAK",
    "bluejays": "TOR",
    "braves": "ATL",
    "brewers": "MIL",
    "cardinals": "STL",
    "cubs": "CHC",
    "dbacks": "ARI",
    "dodgers": "LAD",
    "giants": "SFG",
    "guardians": "CLE",
    "mariners": "SEA",
    "marlins": "MIA",
    "mets": "NYM",
    "nationals": "WSH",
    "orioles": "BAL",
    "padres": "SDP",
    "phillies": "PHI",
    "pirates": "PIT",
    "rangers": "TEX",
    "rays": "TBR",
    "reds": "CIN",
    "redsox": "BOS",
    "rockies": "COL",
    "royals": "KCR",
    "tigers": "DET",
    "twins": "MIN",
    "whitesox": "CHW",
    "yankees": "NYY"
}


@browser(
    headless=False  # Keep browser visible for captcha handling
)
def scrape_team_page(driver: Driver, data: dict):
    """
    Scrape a single team's payroll page for opt-out information
    """
    team_name = data['team_name']
    team_url = data['team_url']
    url = f"https://www.fangraphs.com/roster-resource/payroll/{team_url}"
    
    print(f"[*] Scraping {team_name} ({ABBREV_MAP.get(team_url, team_url)})")
    print(f"    URL: {url}")
    
    driver.get(url)
    
    # Wait for page to load - increased wait time for better reliability
    import time
    time.sleep(8)
    
    # Get page source using Botasaurus Driver method
    html = driver.page_html
    print(f"    [*] Page loaded: {len(html)} characters")
    
    # Save HTML cache
    cache_dir = os.path.join("backend/data/fangraphs_cache/rosterresource")
    os.makedirs(cache_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cache_file = os.path.join(cache_dir, f"fangraphs_optout_{team_url}_{timestamp}.html")
    with open(cache_file, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"    [*] Cached HTML to: {cache_file}")
    
    return html


def parse_opt_outs_from_html(html: str, team_name: str, team_abbr: str):
    """
    Parse opt-out information from Fangraphs payroll page HTML
    Extracts data from embedded JSON in __NEXT_DATA__ script tag
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Extract JSON data from __NEXT_DATA__ script tag
    next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
    
    if not next_data_script:
        print("    [!] No __NEXT_DATA__ script found")
        return results
    
    try:
        import json
        data = json.loads(next_data_script.string)
        
        # Navigate to contract data
        dehydrated_state = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {})
        queries = dehydrated_state.get('queries', [])
        
        if not queries:
            print("    [!] No queries found in dehydrated state")
            return results
        
        # Find the query with contract data
        contract_data = None
        for query in queries:
            query_data = query.get('state', {}).get('data', {})
            if 'dataContract' in query_data:
                contract_data = query_data['dataContract']
                break
        
        if not contract_data:
            print("    [!] No contract data found")
            return results
        
        print(f"    [*] Found {len(contract_data)} player contracts")
        
        # Parse each contract for opt-outs
        for contract in contract_data:
            summary = contract.get('contractSummary', {})
            if not summary:
                continue
            
            player_name = summary.get('playerName')
            contract_years = contract.get('contractYears', [])
            
            opt_outs = []
            
            # Check ContractSummaryPayrollNote for opt-out mentions
            payroll_note = summary.get('ContractSummaryPayrollNote', '')
            if payroll_note and 'opt' in payroll_note.lower():
                print(f"    [*] Checking payroll note for {player_name}: {payroll_note}")
                
                # Extract year from note
                years = re.findall(r'\b(20\d{2})\b', payroll_note)
                
                # Determine type from note
                note_lower = payroll_note.lower()
                opt_type = None
                for type_key, type_code in TYPE_MAPPING.items():
                    if type_key in note_lower:
                        opt_type = type_code
                        break
                
                # If type not specified but contains "opt", default to PO
                if not opt_type and 'opt' in note_lower:
                    opt_type = "PO"
                
                for year in years:
                    opt_outs.append({
                        "season": int(year),
                        "type": opt_type
                    })
            
            # Check each contract year for opt-out notes
            for year_data in contract_years:
                option_notes = year_data.get('OptionNotes', '')
                
                if option_notes and ('opt' in option_notes.lower() or 'void' in option_notes.lower()):
                    season = year_data.get('Season')
                    if season:
                        print(f"    [*] Checking option note for {player_name} ({season}): {option_notes}")
                        
                        # Determine type from notes
                        note_lower = option_notes.lower()
                        opt_type = None
                        for type_key, type_code in TYPE_MAPPING.items():
                            if type_key in note_lower:
                                opt_type = type_code
                                break
                        
                        # If type not specified, default to PO for player opt-outs
                        if not opt_type:
                            opt_type = "PO"
                        
                        opt_outs.append({
                            "season": season,
                            "type": opt_type
                        })
            
            # Only add player if they have opt-outs
            if opt_outs:
                # Deduplicate opt-outs
                seen = set()
                unique_opt_outs = []
                for opt in opt_outs:
                    key = (opt['season'], opt['type'])
                    if key not in seen:
                        seen.add(key)
                        unique_opt_outs.append(opt)
                
                results.append({
                    "player_name": player_name,
                    "team": team_abbr,
                    "opt_outs": unique_opt_outs
                })
                print(f"    [*] Found {len(unique_opt_outs)} opt-out(s) for {player_name}")
        
    except Exception as e:
        print(f"    [!] Error parsing JSON data: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def scrape_all_teams():
    """
    Scrape all 30 MLB teams for opt-out information
    """
    print("="*60)
    print("=== Fangraphs RosterResource Opt-Out Scraper ===")
    print("Starting scrape for all 30 teams...")
    print("="*60)
    
    all_results = []
    total_players_with_optouts = 0
    total_optout_clauses = 0
    
    # Scrape each team
    for i, (team_name, team_url) in enumerate(TEAMS.items(), 1):
        try:
            print(f"\n[{i}/30] Processing {team_name}")
            
            # Use Botasaurus decorator to scrape the page
            html = scrape_team_page({'team_name': team_name, 'team_url': team_url})
            
            # Parse the HTML for opt-outs
            team_abbr = ABBREV_MAP.get(team_url, team_url)
            team_results = parse_opt_outs_from_html(html, team_name, team_abbr)
            
            # Add team info to each result
            for result in team_results:
                result['team'] = team_abbr
                all_results.append(result)
                total_optout_clauses += len(result['opt_outs'])
            
            total_players_with_optouts += len(team_results)
            print(f"    [*] Found {len(team_results)} players with opt-outs for {team_abbr}")
            
            # Small delay between requests
            import time
            time.sleep(2)
            
        except Exception as e:
            print(f"    [!] Error scraping {team_name}: {e}")
            continue
    
    # Save results
    output_dir = "backend/data/fangraphs_cache/rosterresource"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"opt_outs_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=4)
    
    print("\n" + "="*60)
    print("=== Summary ===")
    print(f"Total players with opt-outs: {total_players_with_optouts}")
    print(f"Total opt-out clauses: {total_optout_clauses}")
    print(f"Results saved to: {output_file}")
    print("="*60)
    
    return all_results


def scrape_single_team(team_name: str):
    """
    Scrape a single team for testing
    """
    if team_name not in TEAMS:
        print(f"Error: Team '{team_name}' not found")
        print(f"Available teams: {', '.join(TEAMS.keys())}")
        return
    
    team_url = TEAMS[team_name]
    print("="*60)
    print(f"TESTING OPT-OUT SCRAPER WITH {team_name}")
    print("="*60)
    
    try:
        html = scrape_team_page({'team_name': team_name, 'team_url': team_url})
        team_abbr = ABBREV_MAP.get(team_url, team_url)
        results = parse_opt_outs_from_html(html, team_name, team_abbr)
        
        # Add team info
        for result in results:
            result['team'] = team_abbr
        
        # Save test results
        output_dir = "backend/data/fangraphs_cache/rosterresource"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"test_{team_url}_{timestamp}.json")
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        print("\n" + "="*60)
        print("TEST RESULTS")
        print("="*60)
        print(f"Total players with opt-outs: {len(results)}")
        total_optouts = sum(len(r['opt_outs']) for r in results)
        print(f"Total opt-out clauses: {total_optouts}")
        print(f"Results saved to: {output_file}")
        
        # Print sample results
        if results:
            print("\nSample Results:")
            for result in results[:3]:
                print(f"  - {result['player_name']} ({result['team']}): {len(result['opt_outs'])} opt-out(s)")
        
        print("="*60)
        
    except Exception as e:
        print(f"[!] Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Scrape specific team
        team_name = sys.argv[1]
        scrape_single_team(team_name)
    else:
        # Scrape all teams
        scrape_all_teams()
