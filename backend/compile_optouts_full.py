#!/usr/bin/env python3
"""
Compile opt-outs from full Fangraphs contract data
"""

import json
import re
from datetime import datetime
from collections import defaultdict

def compile_optouts(contracts_file, output_file):
    """
    Compile opt-out information from Fangraphs contract data
    
    Args:
        contracts_file: Path to merged contracts JSON file
        output_file: Path to output opt-outs JSON file
    """
    
    # Load contract data
    with open(contracts_file, 'r') as f:
        contracts = json.load(f)
    
    print(f"Loaded {len(contracts)} player contracts")
    
    # Compile opt-outs
    opt_outs = []
    players_with_optouts = 0
    
    for contract in contracts:
        player_name = contract.get('player_name', '')
        team = contract.get('team', '')
        contract_years = contract.get('contract_years', [])
        
        if not contract_years:
            continue
        
        # Find all opt-outs for this player
        player_opt_outs = []
        
        for year_data in contract_years:
            season = year_data.get('season')
            notes = year_data.get('option_notes', '')
            
            if not notes:
                continue
            
            # Check for opt-out patterns
            opt_outs_in_notes = extract_opt_outs_from_notes(notes, season)
            
            if opt_outs_in_notes:
                player_opt_outs.extend(opt_outs_in_notes)
        
        # Check contract summary for opt-outs too
        contract_summary = contract.get('contract_summary', '')
        if contract_summary:
            summary_opt_outs = extract_opt_outs_from_summary(contract_summary)
            if summary_opt_outs:
                player_opt_outs.extend(summary_opt_outs)
        
        # Remove duplicates (same season and type)
        unique_opt_outs = []
        seen = set()
        for opt in player_opt_outs:
            key = (opt['season'], opt['type'])
            if key not in seen:
                seen.add(key)
                unique_opt_outs.append(opt)
        
        # Sort by season
        unique_opt_outs.sort(key=lambda x: x['season'])
        
        # Add to results if player has opt-outs
        if unique_opt_outs:
            opt_outs.append({
                'player_name': player_name,
                'team': team,
                'opt_outs': unique_opt_outs
            })
            players_with_optouts += 1
    
    # Save results
    with open(output_file, 'w') as f:
        json.dump(opt_outs, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"=== Opt-Out Compilation Complete ===")
    print(f"Total players: {len(contracts)}")
    print(f"Players with opt-outs: {players_with_optouts}")
    print(f"Total opt-out entries: {sum(len(p['opt_outs']) for p in opt_outs)}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*80}")
    
    # Print sample opt-outs
    print(f"\n{'='*80}")
    print(f"=== Sample Opt-Outs (first 10) ===")
    for i, player in enumerate(opt_outs[:10]):
        print(f"{i+1}. {player['player_name']} ({player['team']})")
        for opt in player['opt_outs']:
            print(f"   - {opt['season']}: {opt['type']}")
    print(f"{'='*80}")
    
    return opt_outs


def extract_opt_outs_from_notes(notes: str, season: int):
    """
    Extract opt-out information from option notes
    
    Args:
        notes: Option notes string
        season: Contract year
        
    Returns:
        List of dicts with season and type
    """
    opt_outs = []
    
    if not notes:
        return opt_outs
    
    notes_lower = notes.lower()
    
    # Type mapping
    type_patterns = {
        'player opt-out': 'PO',
        'club opt-out': 'CO',
        'team opt-out': 'CO',
        'mutual opt-out': 'MO'
    }
    
    # Check each pattern
    for pattern, opt_type in type_patterns.items():
        if pattern in notes_lower:
            # If notes contain specific years, extract them
            years = extract_years_from_notes(notes, pattern)
            if years:
                for year in years:
                    opt_outs.append({
                        'season': year,
                        'type': opt_type
                    })
            else:
                # Use the provided season
                opt_outs.append({
                    'season': season,
                    'type': opt_type
                })
    
    return opt_outs


def extract_opt_outs_from_summary(summary: str):
    """
    Extract opt-out information from contract summary
    
    Args:
        summary: Contract summary string
        
    Returns:
        List of dicts with season and type
    """
    opt_outs = []
    
    if not summary:
        return opt_outs
    
    # Look for patterns like "Player opt-out after 2027"
    # or "Club opt-out 2026, 2027"
    
    patterns = [
        # "Player opt-out after 2027"
        r'player\s+opt-out\s+after\s+(\d{4})',
        # "Club opt-out after 2027"
        r'(?:club|team)\s+opt-out\s+after\s+(\d{4})',
        # "Mutual opt-out after 2027"
        r'mutual\s+opt-out\s+after\s+(\d{4})',
        # "Player opt-out 2027"
        r'player\s+opt-out\s+(\d{4})',
        # "Club opt-out 2027"
        r'(?:club|team)\s+opt-out\s+(\d{4})',
        # "Mutual opt-out 2027"
        r'mutual\s+opt-out\s+(\d{4})',
        # Multiple years: "Player opt-outs after 2026, 2027"
        r'player\s+opt-outs?\s+after\s+([\d{4},\s]+)',
        r'(?:club|team)\s+opt-outs?\s+after\s+([\d{4},\s]+)',
        r'mutual\s+opt-outs?\s+after\s+([\d{4},\s]+)',
    ]
    
    type_patterns = {
        'player': 'PO',
        'club': 'CO',
        'team': 'CO',
        'mutual': 'MO'
    }
    
    summary_lower = summary.lower()
    
    for pattern in patterns:
        match = re.search(pattern, summary_lower)
        if match:
            # Determine type based on pattern
            if 'player' in pattern:
                opt_type = 'PO'
            elif 'mutual' in pattern:
                opt_type = 'MO'
            else:
                opt_type = 'CO'
            
            # Extract years
            years_str = match.group(1)
            # Handle comma-separated years
            years = [int(y.strip()) for y in years_str.split(',') if y.strip().isdigit()]
            
            for year in years:
                opt_outs.append({
                    'season': year,
                    'type': opt_type
                })
    
    return opt_outs


def extract_years_from_notes(notes: str, pattern: str):
    """
    Extract specific years from notes that match a pattern
    
    Args:
        notes: Option notes string
        pattern: Pattern to look for (e.g., "player opt-out")
        
    Returns:
        List of years (integers)
    """
    years = []
    
    # Look for patterns like "player opt-out after 2027 and 2028"
    # or "club opt-out 2026, 2027"
    
    # Pattern 1: "after YEAR" or "before YEAR"
    after_match = re.search(rf'{re.escape(pattern)}\s+after\s+(\d{4})', notes, re.IGNORECASE)
    if after_match:
        years.append(int(after_match.group(1)))
    
    # Pattern 2: "YEAR, YEAR" (comma-separated)
    years_match = re.search(rf'{re.escape(pattern)}\s+([\d{4},\s]+)', notes, re.IGNORECASE)
    if years_match and not after_match:  # Only if we didn't already find "after"
        years_str = years_match.group(1)
        years = [int(y.strip()) for y in years_str.split(',') if y.strip().isdigit()]
    
    return years


def analyze_optout_stats(opt_outs_file):
    """
    Analyze opt-out statistics
    
    Args:
        opt_outs_file: Path to opt-outs JSON file
    """
    with open(opt_outs_file, 'r') as f:
        opt_outs = json.load(f)
    
    print(f"\n{'='*80}")
    print(f"=== Opt-Out Statistics ===")
    
    # By type
    type_counts = defaultdict(int)
    for player in opt_outs:
        for opt in player['opt_outs']:
            type_counts[opt['type']] += 1
    
    print(f"\nBy Type:")
    for opt_type, count in sorted(type_counts.items()):
        print(f"  {opt_type}: {count}")
    
    # By team
    team_counts = defaultdict(int)
    for player in opt_outs:
        team_counts[player['team']] += len(player['opt_outs'])
    
    print(f"\nTop 10 Teams by Opt-Outs:")
    for team, count in sorted(team_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {team}: {count}")
    
    # By season
    season_counts = defaultdict(int)
    for player in opt_outs:
        for opt in player['opt_outs']:
            season_counts[opt['season']] += 1
    
    print(f"\nOpt-Outs by Season:")
    for season in sorted(season_counts.keys()):
        print(f"  {season}: {season_counts[season]}")
    
    print(f"{'='*80}")


if __name__ == "__main__":
    # Compile opt-outs from merged contract data
    contracts_file = "backend/data/fangraphs_cache/rosterresource/all_contracts_merged.json"
    output_file = "backend/data/fangraphs_cache/rosterresource/all_optouts.json"
    
    opt_outs = compile_optouts(contracts_file, output_file)
    
    # Analyze statistics
    analyze_optout_stats(output_file)
    
    # Save to output directory as well
    import shutil
    shutil.copy(output_file, "output/fangraphs_optouts.json")
    print(f"\nAlso saved to: output/fangraphs_optouts.json")
