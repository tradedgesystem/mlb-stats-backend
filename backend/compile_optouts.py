#!/usr/bin/env python3
"""
Compile opt-out data from cached HTML files
"""

from bs4 import BeautifulSoup
import re
import json
from datetime import datetime
import os
from glob import glob

# Type mappings for opt-out clauses
TYPE_MAPPING = {
    "player opt-out": "PO",
    "player optout": "PO",
    "club opt-out": "CO",
    "team opt-out": "CO",
    "mutual opt-out": "MO"
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


def parse_opt_outs_from_html(html: str, team_abbr: str):
    """
    Parse opt-out information from Fangraphs payroll page HTML
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Extract JSON data from __NEXT_DATA__ script tag
    next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
    
    if not next_data_script:
        return results
    
    try:
        data = json.loads(next_data_script.string)
        
        # Navigate to contract data
        dehydrated_state = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {})
        queries = dehydrated_state.get('queries', [])
        
        if not queries:
            return results
        
        # Find the query with contract data
        contract_data = None
        for query in queries:
            query_data = query.get('state', {}).get('data', {})
            if 'dataContract' in query_data:
                contract_data = query_data['dataContract']
                break
        
        if not contract_data:
            return results
        
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
        
    except Exception as e:
        print(f"    [!] Error parsing JSON data: {e}")
    
    return results


def compile_all_optouts():
    """
    Compile opt-out data from all cached HTML files
    """
    print("="*60)
    print("=== Compiling Opt-Out Data from Cache ===")
    print("="*60)
    
    cache_dir = "backend/data/fangraphs_cache/rosterresource"
    html_files = glob(os.path.join(cache_dir, "fangraphs_optout_*.html"))
    
    if not html_files:
        print("[!] No cached HTML files found")
        return []
    
    print(f"[*] Found {len(html_files)} cached HTML files")
    
    all_results = []
    
    for html_file in sorted(html_files):
        # Extract team abbreviation from filename
        filename = os.path.basename(html_file)
        # Format: fangraphs_optout_teamname_timestamp.html
        parts = filename.replace('fangraphs_optout_', '').replace('.html', '').split('_')
        team_url = parts[0]
        team_abbr = ABBREV_MAP.get(team_url, team_url.upper())
        
        print(f"\n[*] Processing {team_abbr} from {filename}")
        
        with open(html_file, 'r', encoding='utf-8') as f:
            html = f.read()
        
        team_results = parse_opt_outs_from_html(html, team_abbr)
        
        for result in team_results:
            all_results.append(result)
        
        if team_results:
            print(f"    Found {len(team_results)} players with opt-outs")
    
    # Deduplicate results (player_name + team)
    unique_results = {}
    for result in all_results:
        key = (result['player_name'], result['team'])
        if key not in unique_results:
            unique_results[key] = result
        else:
            # Merge opt_outs if same player appears multiple times
            existing = unique_results[key]
            seen = set((opt['season'], opt['type']) for opt in existing['opt_outs'])
            for opt in result['opt_outs']:
                opt_key = (opt['season'], opt['type'])
                if opt_key not in seen:
                    existing['opt_outs'].append(opt)
                    seen.add(opt_key)
            # Sort opt_outs by season
            existing['opt_outs'].sort(key=lambda x: x['season'])
    
    all_results = list(unique_results.values())
    
    # Sort by team, then player name
    all_results.sort(key=lambda x: (x['team'], x['player_name']))
    
    # Save compiled results
    output_dir = "backend/data/fangraphs_cache/rosterresource"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"opt_outs_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    # Also save to a simpler filename
    latest_file = os.path.join(output_dir, "opt_outs_latest.json")
    with open(latest_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print("\n" + "="*60)
    print("=== Summary ===")
    print(f"Total players with opt-outs: {len(all_results)}")
    total_optouts = sum(len(r['opt_outs']) for r in all_results)
    print(f"Total opt-out clauses: {total_optouts}")
    print(f"Results saved to: {output_file}")
    print(f"Latest copy: {latest_file}")
    print("="*60)
    
    return all_results


if __name__ == "__main__":
    results = compile_all_optouts()
    
    # Print results
    if results:
        print("\n=== Players with Opt-Outs ===")
        for result in results:
            print(f"\n{result['player_name']} ({result['team']}):")
            for opt in result['opt_outs']:
                print(f"  - {opt['season']}: {opt['type']}")
