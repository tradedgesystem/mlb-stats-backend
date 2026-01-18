"""
Script to scrape prospects for all 30 MLB teams
"""

import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from scraper.prospects_scraper import scrape_all_teams, save_to_cache

def main():
    print("=" * 60)
    print("Scraping Top 30 Prospects for All 30 MLB Teams")
    print("=" * 60)
    
    # Scrape all teams
    print("\nStarting scrape process...")
    all_prospects = scrape_all_teams(headless=True)
    
    # Save to cache
    print("\nSaving to cache...")
    save_to_cache(all_prospects)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPE SUMMARY")
    print("=" * 60)
    
    total_prospects = 0
    teams_with_data = 0
    
    for team_key, prospects in all_prospects.items():
        count = len(prospects)
        total_prospects += count
        if count > 0:
            teams_with_data += 1
            print(f"{team_key.ljust(15)}: {count:3d} prospects")
    
    print("-" * 60)
    print(f"Teams with data: {teams_with_data}/30")
    print(f"Total prospects scraped: {total_prospects}")
    print("=" * 60)

if __name__ == "__main__":
    main()
