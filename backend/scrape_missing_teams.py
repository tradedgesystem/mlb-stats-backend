#!/usr/bin/env python3
"""
Scrape the 4 missing teams: TOR, ARI, BOS, CHW
"""

from botasaurus.browser_decorator import browser
from botasaurus_driver import Driver
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os

# Just the 4 missing teams
MISSING_TEAMS = {
    "Toronto Blue Jays": "bluejays",
    "Arizona Diamondbacks": "dbacks",
    "Boston Red Sox": "redsox",
    "Chicago White Sox": "whitesox"
}

# Abbreviation mappings
ABBREV_MAP = {
    "bluejays": "TOR",
    "dbacks": "ARI",
    "redsox": "BOS",
    "whitesox": "CHW"
}


def parse_contract_data(html: str, team_name: str, team_abbr: str):
    """
    Parse complete contract data from Fangraphs payroll page HTML
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Extract JSON data from __NEXT_DATA__ script tag
    next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
    
    if not next_data_script:
        print("    [!] No __NEXT_DATA__ script found")
        return results
    
    try:
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
        
        # Parse each contract
        for contract in contract_data:
            summary = contract.get('contractSummary', {})
            if not summary:
                continue
            
            player_name = summary.get('playerName')
            
            if not player_name:
                continue
            
            # Extract player details
            player_data = {
                "player_name": player_name,
                "team": team_abbr,
                "full_team_name": team_name
            }
            
            # Add AAV
            aav = summary.get('AAV')
            if aav:
                player_data['aav'] = aav
            
            # Parse contract years
            contract_years = contract.get('contractYears', [])
            if contract_years:
                player_data['contract_years'] = []
                
                for year_data in contract_years:
                    season = year_data.get('Season')
                    if not season:
                        continue
                    
                    year_entry = {
                        "season": season,
                        "type": year_data.get('Type', ''),
                        "salary": year_data.get('Salary')
                    }
                    
                    # Add option buyout if available
                    buyout = year_data.get('OptionBuyout')
                    if buyout:
                        year_entry['option_buyout'] = buyout
                    
                    # Add option notes if available (THIS IS WHAT WE NEED!)
                    option_notes = year_data.get('OptionNotes')
                    if option_notes:
                        year_entry['option_notes'] = option_notes
                    
                    # Add team ID if available
                    team_id = year_data.get('TeamId')
                    if team_id:
                        year_entry['team_id'] = team_id
                    
                    player_data['contract_years'].append(year_entry)
            
            results.append(player_data)
        
    except Exception as e:
        print(f"    [!] Error parsing JSON data: {e}")
        import traceback
        traceback.print_exc()
    
    return results


@browser(
    headless=False
)
def scrape_missing_teams(driver: Driver, data=None):
    """
    Scrape the 4 missing teams: TOR, ARI, BOS, CHW
    """
    print("="*80)
    print("=== Scraping 4 Missing Teams ===")
    print("="*80)
    
    all_results = []
    
    for i, (team_name, team_url) in enumerate(MISSING_TEAMS.items(), 1):
        try:
            print(f"\n[{i}/4] Processing {team_name}")
            
            # Navigate to team URL
            url = f"https://www.fangraphs.com/roster-resource/payroll/{team_url}"
            print(f"    URL: {url}")
            
            driver.get(url)
            
            # Wait for page to load
            import time
            time.sleep(3)
            
            # Get page source
            html = driver.page_html
            print(f"    [*] Page loaded: {len(html)} characters")
            
            # Parse contract data
            team_abbr = ABBREV_MAP.get(team_url, team_url)
            team_results = parse_contract_data(html, team_name, team_abbr)
            
            print(f"    [*] Found {len(team_results)} players for {team_abbr}")
            
            all_results.extend(team_results)
            
            # Save cache for this team
            cache_dir = "backend/data/fangraphs_cache/rosterresource"
            os.makedirs(cache_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cache_file = os.path.join(cache_dir, f"contracts_{team_url}_{timestamp}.html")
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(html)
            
        except Exception as e:
            print(f"    [!] Error scraping {team_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save results
    output_dir = "backend/data/fangraphs_cache/rosterresource"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"contracts_missing_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print("\n" + "="*80)
    print(f"=== Summary ===")
    print(f"Total players scraped: {len(all_results)}")
    print(f"Results saved to: {output_file}")
    print("="*80)
    
    return all_results


if __name__ == "__main__":
    scrape_missing_teams()
