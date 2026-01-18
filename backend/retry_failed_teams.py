#!/usr/bin/env python3
"""
Retry script for failed Fangraphs contract scrapes
"""

from botasaurus.browser_decorator import browser
from botasaurus_driver import Driver
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime
import os
import time

# Team mappings for retry
FAILED_TEAMS = {
    "Los Angeles Angels": ("angels", "LAA"),
    "Toronto Blue Jays": ("bluejays", "TOR"),
    "Arizona Diamondbacks": ("dbacks", "ARI"),
    "Boston Red Sox": ("redsox", "BOS"),
    "Chicago White Sox": ("whitesox", "CHW")
}

@browser(headless=False)
def scrape_failed_teams(driver: Driver, data=None):
    """
    Retry scraping for failed teams with enhanced error handling
    """
    print("="*80)
    print("=== Retrying Failed Teams ===")
    print("="*80)
    
    all_results = []
    
    # Retry each failed team
    for i, (team_name, (team_url, team_abbr)) in enumerate(FAILED_TEAMS.items(), 1):
        print(f"\n[{i}/5] Retrying {team_name}")
        
        for attempt in range(1, 4):  # 3 attempts per team
            try:
                url = f"https://www.fangraphs.com/roster-resource/payroll/{team_url}"
                print(f"    Attempt {attempt}: {url}")
                
                driver.get(url)
                
                # Wait longer for page load
                time.sleep(5)
                
                # Wait for __NEXT_DATA__ to be present
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, '__NEXT_DATA__'))
                    )
                    print(f"    [*] __NEXT_DATA__ found after wait")
                except:
                    print(f"    [!] __NEXT_DATA__ not found after 10s, proceeding anyway")
                
                html = driver.page_html
                print(f"    [*] Page loaded: {len(html)} characters")
                
                # Check if page is too small (likely an error page)
                if len(html) < 100000:
                    print(f"    [!] Page too small ({len(html)} chars), likely error page")
                    if attempt < 3:
                        print(f"    [*] Waiting before retry...")
                        time.sleep(5)
                        continue
                    else:
                        print(f"    [!] Max attempts reached, skipping {team_name}")
                        break
                
                # Parse contract data
                team_results = parse_contract_data(html, team_name, team_abbr)
                
                if not team_results:
                    print(f"    [!] No players found in parsing")
                    if attempt < 3:
                        print(f"    [*] Waiting before retry...")
                        time.sleep(5)
                        continue
                    else:
                        print(f"    [!] Max attempts reached, skipping {team_name}")
                        break
                
                print(f"    [*] SUCCESS: Found {len(team_results)} players for {team_abbr}")
                
                all_results.extend(team_results)
                
                # Save cache
                cache_dir = "backend/data/fangraphs_cache/rosterresource"
                os.makedirs(cache_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                cache_file = os.path.join(cache_dir, f"contracts_{team_url}_retry_{timestamp}.html")
                with open(cache_file, 'w', encoding='utf-8') as f:
                    f.write(html)
                
                break  # Success, move to next team
                
            except Exception as e:
                print(f"    [!] Attempt {attempt} failed: {e}")
                import traceback
                traceback.print_exc()
                
                if attempt < 3:
                    print(f"    [*] Waiting before retry...")
                    time.sleep(10)
                else:
                    print(f"    [!] Max attempts reached, skipping {team_name}")
                    break
    
    # Save results
    output_dir = "backend/data/fangraphs_cache/rosterresource"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"retry_teams_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print("\n" + "="*80)
    print("=== Retry Summary ===")
    print(f"Total players scraped: {len(all_results)}")
    print(f"Results saved to: {output_file}")
    print("="*80)
    
    return all_results


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
        
        # Find query with contract data
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
            
            player_info = summary.get('playerInfo', {})
            player_name = summary.get('playerName')
            
            if not player_name:
                continue
            
            # Extract player details
            player_data = {
                "player_name": player_name,
                "team": team_abbr,
                "full_team_name": team_name
            }
            
            # Add age if available
            if player_info:
                age = player_info.get('Age')
                service_time = player_info.get('ServiceTime')
                if age:
                    player_data['age'] = age
                if service_time:
                    player_data['service_time'] = service_time
            
            # Add contract summary
            contract_summary = summary.get('ContractSummary')
            if contract_summary:
                player_data['contract_summary'] = contract_summary
            
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
                    
                    # Add option notes if available
                    option_notes = year_data.get('OptionNotes')
                    if option_notes:
                        year_entry['option_notes'] = option_notes
                    
                    # Add team ID if available
                    team_id = year_data.get('TeamID')
                    if team_id:
                        year_entry['team_id'] = team_id
                    
                    player_data['contract_years'].append(year_entry)
            
            results.append(player_data)
        
    except Exception as e:
        print(f"    [!] Error parsing JSON data: {e}")
        import traceback
        traceback.print_exc()
    
    return results


if __name__ == "__main__":
    scrape_failed_teams()
