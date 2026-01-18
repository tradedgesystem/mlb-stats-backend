# Fangraphs RosterResource Contract Data Extraction Guide

## Overview
This guide explains how to extract complete contract data from Fangraphs RosterResource payroll pages using Botasaurus with a headless/visible browser.

## Quick Start

### Prerequisites
Install required dependencies:
```bash
pip install botasaurus botasaurus-driver bs4
```

### Run the Scraper
Navigate to project directory and run:
```bash
cd "/Users/andresmartinez/MLB Stats"
python3 backend/scraper/fangraphs_contracts_scraper.py
```

## What the Scraper Does

### 1. Single Browser Session
- Opens ONE browser instance for all 30 teams
- Navigates to each team's payroll page sequentially
- Much more efficient than opening/closing browser per team
- Reduces detection risk

### 2. Data Extraction Strategy
The scraper extracts data from Fangraphs' embedded JSON (`__NEXT_DATA__` script tag) rather than parsing HTML tables. This is more reliable because:

- Fangraphs uses Next.js which stores data in JavaScript
- HTML tables may not contain all contract details
- JSON provides complete year-by-year breakdowns
- Includes option types, buyouts, and notes

### 3. Data Structure Extracted
For each player, the scraper captures:
- **player_name**: Player's name
- **team**: Team abbreviation (e.g., "NYY", "LAD")
- **full_team_name**: Complete team name
- **aav**: Average Annual Value
- **age**: Player's age (if available)
- **service_time**: Years of MLB service
- **contract_years**: Array of contract year objects with:
  - **season**: Contract year
  - **type**: Contract type (GUARANTEED, PLAYER OPTION, CLUB OPTION, MUTUAL OPTION, OPT OUT, FREE AGENT, ARB 1-4, PRE-ARB, VESTING, etc.)
  - **salary**: Salary amount for that year
  - **option_buyout**: Option buyout amount (if applicable)
  - **option_notes**: Additional option details
  - **team_id**: Team identifier for that year

### 4. Caching
- Saves raw HTML for each team as `contracts_{team}_{timestamp}.html`
- Saves compiled JSON as `all_contracts_{timestamp}.json`
- All files stored in `backend/data/fangraphs_cache/rosterresource/`

## Output File Location

### Main Output
```
backend/data/fangraphs_cache/rosterresource/all_contracts_{timestamp}.json
```

### Cache Files
```
backend/data/fangraphs_cache/rosterresource/contracts_angels_{timestamp}.html
backend/data/fangraphs_cache/rosterresource/contracts_astros_{timestamp}.html
...
```

## Key Implementation Details

### Team Mappings
The scraper includes mappings for all 30 teams:
- Full team name → URL slug (e.g., "New York Yankees" → "yankees")
- URL slug → Team abbreviation (e.g., "yankees" → "NYY")

### URL Pattern
```
https://www.fangraphs.com/roster-resource/payroll/{team_url}
```

### Parsing Logic
```python
# 1. Find __NEXT_DATA__ script tag
next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})

# 2. Parse JSON
data = json.loads(next_data_script.string)

# 3. Navigate through nested structure
dehydrated_state = data['props']['pageProps']['dehydratedState']
queries = dehydrated_state['queries']

# 4. Find contract data
for query in queries:
    if 'dataContract' in query['state']['data']:
        contract_data = query['state']['data']['dataContract']

# 5. Extract player info and contract years
for contract in contract_data:
    summary = contract['contractSummary']
    player_name = summary['playerName']
    contract_years = summary['contractYears']
```

## Customization Options

### Change Browser Visibility
Edit the `@browser` decorator:
```python
@browser(headless=False)  # True for headless, False for visible
```
- Use `headless=False` when you need to see the browser (debugging, captcha handling)
- Use `headless=True` for faster, invisible operation

### Scrape Specific Teams
Modify `TEAMS` dictionary to only include teams you want:
```python
TEAMS = {
    "New York Yankees": "yankees",
    "Boston Red Sox": "redsox",
    # ... add/remove teams as needed
}
```

### Change Wait Time Between Pages
Adjust the sleep duration after page load:
```python
time.sleep(3)  # Increase if pages load slowly
time.sleep(5)  # For slower connections
```

### Change Cache Location
Modify cache directory path:
```python
cache_dir = "backend/data/fangraphs_cache/rosterresource"
# Change to custom location
cache_dir = "custom/cache/location"
```

## Troubleshooting

### Issue: "No __NEXT_DATA__ script found"
**Cause**: Page didn't load completely or structure changed

**Solution**: 
- Increase `time.sleep()` duration
- Check browser is visible and page loaded
- Verify Fangraphs site structure hasn't changed
- Look for new script tag patterns in HTML

### Issue: "No contract data found"
**Cause**: Fangraphs changed data structure

**Solution**:
- Print `dehydrated_state` to debug
- Check what keys are available in `queries`
- Update parsing logic to match new structure
- Examine raw HTML to find new data location

### Issue: Scraper stops after a few teams
**Cause**: Browser crashed or blocked

**Solution**:
- Use headless mode (less resource intensive)
- Add longer delays between teams
- Check for CAPTCHA on pages
- Try running during off-peak hours

### Issue: Missing player data
**Cause**: Some players don't have contracts listed yet

**Solution**: Normal - pre-arb players won't have detailed contracts
- Check if player appears on Fangraphs site manually
- Verify scraper didn't skip due to error

## Data Transformation Tips

### Extract Opt-Outs Only
If you only want opt-out data, filter the JSON:

```python
import json

# Load data
with open('all_contracts_20260117_131727.json', 'r') as f:
    data = json.load(f)

# Filter for opt-outs
opt_outs = []
for player in data:
    opt_out_years = [
        {
            "season": year['season'],
            "type": "OPT OUT"
        }
        for year in player.get('contract_years', [])
        if 'OPT OUT' in year.get('type', '')
        or 'PLAYER OPTION' in year.get('type', '')
    ]
    if opt_out_years:
        opt_outs.append({
            "player_name": player['player_name'],
            "team": player['team'],
            "opt_outs": opt_out_years
        })

# Save
with open('opt_outs_only.json', 'w') as f:
    json.dump(opt_outs, f, indent=2)
```

### Convert to CSV
```python
import json
import csv

with open('all_contracts_20260117_131727.json', 'r') as f:
    data = json.load(f)

with open('contracts.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['player_name', 'team', 'season', 'type', 'salary'])
    
    for player in data:
        for year in player.get('contract_years', []):
            writer.writerow([
                player['player_name'],
                player['team'],
                year['season'],
                year['type'],
                year['salary']
            ])
```

### Calculate Team Payroll
```python
from collections import defaultdict
import json

with open('all_contracts_20260117_131727.json', 'r') as f:
    data = json.load(f)

team_payroll = defaultdict(list)

for player in data:
    team = player['team']
    for year in player.get('contract_years', []):
        if year.get('salary') and year['season'] == 2026:
            team_payroll[team].append(year['salary'])

# Print totals
for team, salaries in team_payroll.items():
    total = sum(salaries)
    print(f"{team}: ${total:,.0f}")
```

### Extract Players with Specific Contract Types
```python
import json

# Load data
with open('all_contracts_20260117_131727.json', 'r') as f:
    data = json.load(f)

# Find players with player options
player_options = []
for player in data:
    has_player_option = any(
        'PLAYER OPTION' in year.get('type', '')
        for year in player.get('contract_years', [])
    )
    if has_player_option:
        player_options.append({
            "player_name": player['player_name'],
            "team": player['team'],
            "aav": player.get('aav')
        })

# Save
with open('players_with_options.json', 'w') as f:
    json.dump(player_options, f, indent=2)
```

## Best Practices

1. **Run during off-peak hours** - Fangraphs may block during high traffic
2. **Use headless mode for production** - Faster and less resource intensive
3. **Save cache files** - Keep raw HTML for debugging
4. **Verify data samples** - Check a few records manually against Fangraphs
5. **Version control** - Commit scraper code and track changes
6. **Document customizations** - Note what you modified and why

## Advanced: Adding New Data Fields

To extract additional data from the contract JSON:

1. Identify new field in `contract['contractSummary']`
2. Add to `player_data` dictionary:
```python
player_data = {
    "player_name": player_name,
    "team": team_abbr,
    "full_team_name": team_name,
    # Add new fields here
    "new_field": summary.get('NewField')
}
```
3. Update the return structure in `parse_contract_data()`

## Future Maintenance

### Signs Fangraphs May Have Changed
- "No __NEXT_DATA__ script found" errors
- "No contract data found" for all teams
- JSON parsing errors
- Missing expected fields

### How to Update
1. Load a team page HTML in browser
2. Use DevTools to inspect `__NEXT_DATA__` script tag
3. Compare structure with current scraper
4. Update parsing logic to match new structure
5. Test with one team before running all 30

## Opt-Out Type Mapping

The scraper extracts the following contract types that may indicate opt-outs:

- **OPT OUT** - General opt-out clauses
- **PLAYER OPTION** - Player can opt out (maps to PO)
- **CLUB OPTION** - Team can opt out (maps to CO)
- **MUTUAL OPTION** - Either party can opt out (maps to MO)

To convert these types to the standardized format (PO, CO, MO):

```python
OPT_OUT_TYPE_MAPPING = {
    "OPT OUT": "PO",
    "PLAYER OPTION": "PO",
    "CLUB OPTION": "CO",
    "MUTUAL OPTION": "MO"
}

def normalize_opt_out_type(type_str):
    return OPT_OUT_TYPE_MAPPING.get(type_str, type_str)
```

## Example Output Structure

```json
[
  {
    "player_name": "Juan Soto",
    "team": "NYM",
    "full_team_name": "New York Mets",
    "aav": 51000000,
    "age": 26,
    "service_time": 6,
    "contract_years": [
      {
        "season": 2025,
        "type": "GUARANTEED",
        "salary": 61875000
      },
      {
        "season": 2030,
        "type": "OPT OUT",
        "salary": 46000000,
        "option_notes": "Player can opt out after 2030 season"
      }
    ]
  }
]
```

## Summary

This scraper efficiently extracts complete contract data from all 30 MLB teams using Botasaurus with a single browser session. The data structure is comprehensive and includes year-by-year breakdowns, option types, salaries, and buyout amounts. The scraper is modular and can be customized for specific needs.

## Related Files

- `backend/scraper/fangraphs_contracts_scraper.py` - Main scraper script
- `backend/scraper/fangraphs_optout_scraper.py` - Original opt-out specific scraper
- `backend/compile_optouts.py` - Utility for compiling opt-out data

## Notes

- The scraper uses the same browser session for all 30 teams for efficiency
- HTML cache files are saved for each team for debugging purposes
- The final output is a single JSON file with all players from all teams
- Team abbreviations follow standard MLB abbreviations (NYY, LAD, BOS, etc.)
- Contract types are extracted exactly as they appear on Fangraphs
