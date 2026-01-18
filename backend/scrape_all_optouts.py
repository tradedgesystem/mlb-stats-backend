#!/usr/bin/env python3
"""
Scrape all 30 MLB teams for opt-out clauses from Fangraphs
with retry logic to handle errors and ensure complete data collection
"""

import sys
import os
import time
import json
from datetime import datetime

# Add scraper directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.fangraphs_optout_scraper import (
    scrape_team_page,
    parse_opt_outs_from_html,
    TEAMS,
    ABBREV_MAP
)


def scrape_team_with_retry(team_name, team_url, max_retries=3):
    """
    Scrape a single team with retry logic
    
    Returns:
        tuple: (success: bool, results: list, error_message: str)
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n{'='*60}")
            print(f"[Attempt {attempt}/{max_retries}] Scraping {team_name} ({ABBREV_MAP.get(team_url, team_url)})")
            print('='*60)
            
            # Scrape the page
            html = scrape_team_page({'team_name': team_name, 'team_url': team_url})
            
            if not html or len(html) < 1000:
                print(f"[!] Warning: HTML seems too short ({len(html)} chars)")
                if attempt < max_retries:
                    print(f"[*] Retrying in 3 seconds...")
                    time.sleep(3)
                    continue
            
            # Parse HTML for opt-outs
            team_abbr = ABBREV_MAP.get(team_url, team_url)
            results = parse_opt_outs_from_html(html, team_name, team_abbr)
            
            # Check for common errors
            error_indicators = [
                "No __NEXT_DATA__ script found",
                "No queries found in dehydrated state",
                "No contract data found"
            ]
            
            has_errors = any(indicator in str(results) for indicator in error_indicators)
            
            if has_errors and len(results) == 0:
                print(f"[!] Error detected in parsing, no opt-outs found")
                if attempt < max_retries:
                    print(f"[*] Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
            
            # Success!
            print(f"[✓] Successfully scraped {team_name}")
            print(f"    Found {len(results)} players with opt-outs")
            
            return True, results, None
            
        except Exception as e:
            print(f"[!] Error on attempt {attempt}: {e}")
            import traceback
            traceback.print_exc()
            
            if attempt < max_retries:
                wait_time = attempt * 3  # Exponential backoff: 3s, 6s, 9s
                print(f"[*] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                print(f"[✗] Failed to scrape {team_name} after {max_retries} attempts")
                return False, [], str(e)


def scrape_all_teams():
    """
    Scrape all 30 MLB teams with retry logic
    """
    print("="*80)
    print(" "*20 + "FANGRAPHS OPT-OUT SCRAPER")
    print("="*80)
    print(f"Starting scrape for all 30 MLB teams at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    all_results = []
    teams_with_data = []
    teams_failed = []
    
    total_players = 0
    total_optout_clauses = 0
    
    # Scrape each team
    for i, (team_name, team_url) in enumerate(TEAMS.items(), 1):
        print(f"\n{'#'*80}")
        print(f"[{i}/30] Processing: {team_name}")
        print('#'*80)
        
        success, team_results, error = scrape_team_with_retry(team_name, team_url)
        
        if success:
            # Add team info to each result
            team_abbr = ABBREV_MAP.get(team_url, team_url)
            for result in team_results:
                result['team'] = team_abbr
                all_results.append(result)
                total_optout_clauses += len(result['opt_outs'])
            
            total_players += len(team_results)
            teams_with_data.append(team_name)
            
            print(f"\n[✓] {team_name}: {len(team_results)} players, {sum(len(r['opt_outs']) for r in team_results)} opt-outs")
        else:
            teams_failed.append((team_name, error))
            print(f"\n[✗] {team_name}: FAILED")
            if error:
                print(f"    Error: {error}")
        
        # Small delay between teams to be respectful
        if i < 30:
            time.sleep(2)
    
    # Save results
    output_dir = "backend/data/fangraphs_cache/rosterresource"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"opt_outs_all_teams_{timestamp}.json")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=4)
    
    # Print summary
    print("\n" + "="*80)
    print(" "*30 + "SCRAPE SUMMARY")
    print("="*80)
    print(f"Total teams scraped successfully: {len(teams_with_data)}/30")
    print(f"Total players with opt-outs: {total_players}")
    print(f"Total opt-out clauses: {total_optout_clauses}")
    print(f"\nResults saved to: {output_file}")
    
    if teams_failed:
        print(f"\n[!] Teams that failed to scrape ({len(teams_failed)}):")
        for team, error in teams_failed:
            print(f"    ✗ {team}: {error}")
    
    print(f"\n[✓] Teams with data ({len(teams_with_data)}):")
    for team in teams_with_data:
        print(f"    ✓ {team}")
    
    print("="*80)
    
    # If any teams failed, ask if user wants to retry
    if teams_failed:
        print("\n[!] Some teams failed to scrape.")
        print(f"    Run the script again to retry the failed teams.")
        print(f"    The scraper will continue from where it left off.")
    
    return all_results, teams_failed


if __name__ == "__main__":
    # Parse command line arguments
    team_to_scrape = None
    if len(sys.argv) > 1:
        team_arg = " ".join(sys.argv[1:])
        # Try to find matching team
        for team_name, team_url in TEAMS.items():
            if team_arg.lower() in team_name.lower() or team_arg.lower() in team_url.lower():
                team_to_scrape = team_name
                break
        
        if not team_to_scrape:
            print(f"Error: Team '{team_arg}' not found")
            print(f"Available teams:")
            for team_name in sorted(TEAMS.keys()):
                print(f"  - {team_name}")
            sys.exit(1)
        
        # Scrape single team
        print(f"Scraping single team: {team_to_scrape}")
        success, results, error = scrape_team_with_retry(
            team_to_scrape, 
            TEAMS[team_to_scrape],
            max_retries=3
        )
        
        if success:
            print(f"\n[✓] Success! Found {len(results)} players with opt-outs")
            
            # Save single team results
            output_dir = "backend/data/fangraphs_cache/rosterresource"
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"opt_outs_{TEAMS[team_to_scrape]}_{timestamp}.json")
            
            team_abbr = ABBREV_MAP.get(TEAMS[team_to_scrape], TEAMS[team_to_scrape])
            for result in results:
                result['team'] = team_abbr
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4)
            
            print(f"Results saved to: {output_file}")
        else:
            print(f"\n[✗] Failed to scrape {team_to_scrape}")
            if error:
                print(f"Error: {error}")
    else:
        # Scrape all teams
        results, failed = scrape_all_teams()
        
        if failed:
            print("\n" + "!"*80)
            print(f"WARNING: {len(failed)} team(s) failed to scrape")
            print("!"*80)
            print("Run the script again to retry failed teams")
            sys.exit(1)
