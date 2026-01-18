"""
MLB Team Prospect URL mappings for scraping
Each team has a prospects page at mlb.com/milb/prospects/{team_slug}
"""

TEAMS = {
    # American League East
    "yankees": {
        "slug": "yankees",
        "full_name": "New York Yankees",
        "abbreviation": "NYY",
        "mlb_id": 147
    },
    "redsox": {
        "slug": "redsox",
        "full_name": "Boston Red Sox",
        "abbreviation": "BOS",
        "mlb_id": 111
    },
    "rays": {
        "slug": "rays",
        "full_name": "Tampa Bay Rays",
        "abbreviation": "TBR",
        "mlb_id": 139
    },
    "orioles": {
        "slug": "orioles",
        "full_name": "Baltimore Orioles",
        "abbreviation": "BAL",
        "mlb_id": 110
    },
    "bluejays": {
        "slug": "bluejays",
        "full_name": "Toronto Blue Jays",
        "abbreviation": "TOR",
        "mlb_id": 141
    },
    
    # American League Central
    "guardians": {
        "slug": "guardians",
        "full_name": "Cleveland Guardians",
        "abbreviation": "CLE",
        "mlb_id": 114
    },
    "whitesox": {
        "slug": "whitesox",
        "full_name": "Chicago White Sox",
        "abbreviation": "CHW",
        "mlb_id": 145
    },
    "tigers": {
        "slug": "tigers",
        "full_name": "Detroit Tigers",
        "abbreviation": "DET",
        "mlb_id": 116
    },
    "royals": {
        "slug": "royals",
        "full_name": "Kansas City Royals",
        "abbreviation": "KCR",
        "mlb_id": 118
    },
    "twins": {
        "slug": "twins",
        "full_name": "Minnesota Twins",
        "abbreviation": "MIN",
        "mlb_id": 142
    },
    
    # American League West
    "astros": {
        "slug": "astros",
        "full_name": "Houston Astros",
        "abbreviation": "HOU",
        "mlb_id": 117
    },
    "angels": {
        "slug": "angels",
        "full_name": "Los Angeles Angels",
        "abbreviation": "LAA",
        "mlb_id": 108
    },
    "rangers": {
        "slug": "rangers",
        "full_name": "Texas Rangers",
        "abbreviation": "TEX",
        "mlb_id": 140
    },
    "athletics": {
        "slug": "athletics",
        "full_name": "Oakland Athletics",
        "abbreviation": "OAK",
        "mlb_id": 133
    },
    "mariners": {
        "slug": "mariners",
        "full_name": "Seattle Mariners",
        "abbreviation": "SEA",
        "mlb_id": 136
    },
    
    # National League East
    "braves": {
        "slug": "braves",
        "full_name": "Atlanta Braves",
        "abbreviation": "ATL",
        "mlb_id": 144
    },
    "mets": {
        "slug": "mets",
        "full_name": "New York Mets",
        "abbreviation": "NYM",
        "mlb_id": 121
    },
    "phillies": {
        "slug": "phillies",
        "full_name": "Philadelphia Phillies",
        "abbreviation": "PHI",
        "mlb_id": 143
    },
    "marlins": {
        "slug": "marlins",
        "full_name": "Miami Marlins",
        "abbreviation": "MIA",
        "mlb_id": 146
    },
    "nationals": {
        "slug": "nationals",
        "full_name": "Washington Nationals",
        "abbreviation": "WSH",
        "mlb_id": 120
    },
    
    # National League Central
    "cardinals": {
        "slug": "cardinals",
        "full_name": "St. Louis Cardinals",
        "abbreviation": "STL",
        "mlb_id": 138
    },
    "cubs": {
        "slug": "cubs",
        "full_name": "Chicago Cubs",
        "abbreviation": "CHC",
        "mlb_id": 112
    },
    "brewers": {
        "slug": "brewers",
        "full_name": "Milwaukee Brewers",
        "abbreviation": "MIL",
        "mlb_id": 158
    },
    "pirates": {
        "slug": "pirates",
        "full_name": "Pittsburgh Pirates",
        "abbreviation": "PIT",
        "mlb_id": 134
    },
    "reds": {
        "slug": "reds",
        "full_name": "Cincinnati Reds",
        "abbreviation": "CIN",
        "mlb_id": 113
    },
    
    # National League West
    "dodgers": {
        "slug": "dodgers",
        "full_name": "Los Angeles Dodgers",
        "abbreviation": "LAD",
        "mlb_id": 119
    },
    "giants": {
        "slug": "giants",
        "full_name": "San Francisco Giants",
        "abbreviation": "SFG",
        "mlb_id": 137
    },
    "padres": {
        "slug": "padres",
        "full_name": "San Diego Padres",
        "abbreviation": "SDP",
        "mlb_id": 135
    },
    "rockies": {
        "slug": "rockies",
        "full_name": "Colorado Rockies",
        "abbreviation": "COL",
        "mlb_id": 115
    },
    "dbacks": {
        "slug": "dbacks",
        "full_name": "Arizona Diamondbacks",
        "abbreviation": "ARI",
        "mlb_id": 109
    }
}

def get_team_url(team_key: str) -> str:
    """Get the prospects URL for a specific team"""
    if team_key not in TEAMS:
        raise ValueError(f"Unknown team: {team_key}")
    return f"https://www.mlb.com/milb/prospects/{TEAMS[team_key]['slug']}"

def get_all_team_urls() -> list[tuple[str, str]]:
    """Get all team URLs with their keys"""
    return [(key, get_team_url(key)) for key in TEAMS.keys()]
