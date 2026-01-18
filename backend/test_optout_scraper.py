"""
Test script for Fangraphs Opt-Out Scraper
Tests with one team to verify HTML structure and handle captchas
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scraper.fangraphs_optout_scraper import FangraphsOptOutScraper
from scraper.team_urls import TEAMS

if __name__ == "__main__":
    # Test with Yankees first
    test_team = 'yankees'
    
    print(f"\n{'='*60}")
    print(f"TESTING OPT-OUT SCRAPER WITH {TEAMS[test_team]['full_name']}")
    print(f"{'='*60}")
    
    scraper = FangraphsOptOutScraper()
    
    # Start browser
    scraper.bot.start_session()
    
    # Scrape one team
    results = scraper.scrape_team_payroll(test_team, TEAMS[test_team])
    
    # Close browser
    scraper.bot.close_session()
    
    # Print results
    print(f"\n{'='*60}")
    print(f"TEST RESULTS")
    print(f"{'='*60}")
    print(f"Total players with opt-outs: {len(results)}")
    print(f"Total opt-out clauses: {sum(len(p['opt_outs']) for p in results)}")
    
    if results:
        print(f"\n{'='*60}")
        print(f"DETAILED RESULTS")
        print(f"{'='*60}")
        for player in results:
            print(f"\n{player['player_name']} ({player['team']}):")
            for opt in player['opt_outs']:
                print(f"  - Season {opt['season']}: {opt['type']}")
        
        # Save test results
        scraper.save_results(results, 'test_yankees_optouts.json')
    else:
        print("\nNo opt-outs found. Check the HTML structure.")
