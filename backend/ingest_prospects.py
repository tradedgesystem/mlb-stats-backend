"""
Ingest MLB prospect data into SQLite database
"""

import sqlite3
import json
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).with_name("stats.db")


def create_prospects_table():
    """Create the prospects table if it doesn't exist"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prospects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        team TEXT NOT NULL,
        team_abbreviation TEXT NOT NULL,
        mlb_id INTEGER,
        system_rank INTEGER NOT NULL,
        top_100_rank INTEGER,
        fv_value TEXT,
        position TEXT,
        age INTEGER,
        level TEXT,
        eta TEXT,
        data_source TEXT NOT NULL,
        last_updated TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_name, team, system_rank)
    )
    """)
    
    # Create indexes for common queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_prospects_team 
    ON prospects(team)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_prospects_system_rank 
    ON prospects(team, system_rank)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_prospects_top_100 
    ON prospects(top_100_rank) 
    WHERE top_100_rank IS NOT NULL
    """)
    
    conn.commit()
    conn.close()
    
    print("Created prospects table successfully")


def ingest_prospects(prospects_data: Dict[str, List[Dict]]) -> int:
    """
    Ingest prospect data into database
    
    Args:
        prospects_data: Dictionary mapping team keys to prospect lists
        
    Returns:
        Number of prospects ingested
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    total_ingested = 0
    total_updated = 0
    
    for team_key, team_prospects in prospects_data.items():
        for prospect in team_prospects:
            try:
                cursor.execute("""
                INSERT OR REPLACE INTO prospects 
                (player_name, team, team_abbreviation, mlb_id, system_rank, 
                 top_100_rank, fv_value, position, age, level, eta, 
                 data_source, last_updated, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        COALESCE((SELECT created_at FROM prospects 
                                  WHERE player_name = ? AND team = ? AND system_rank = ?), CURRENT_TIMESTAMP))
                """, (
                    prospect['player_name'],
                    prospect['team'],
                    prospect['team_abbreviation'],
                    prospect['mlb_id'],
                    prospect['system_rank'],
                    prospect['top_100_rank'],
                    prospect['fv_value'],
                    prospect['position'],
                    prospect['age'],
                    prospect['level'],
                    prospect['eta'],
                    prospect['data_source'],
                    prospect['last_updated'],
                    prospect['player_name'],
                    prospect['team'],
                    prospect['system_rank']
                ))
                
                total_ingested += 1
                
            except Exception as e:
                print(f"Error ingesting prospect {prospect['player_name']}: {e}")
                continue
    
    conn.commit()
    conn.close()
    
    print(f"Ingested {total_ingested} prospects from {len(prospects_data)} teams")
    return total_ingested


def get_prospects_by_team(team: str, limit: Optional[int] = None) -> List[Dict]:
    """Get prospects for a specific team"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM prospects WHERE team = ? ORDER BY system_rank ASC"
    params = [team]
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    prospects = [dict(row) for row in rows]
    conn.close()
    
    return prospects


def get_top_100_prospects() -> List[Dict]:
    """Get all prospects in the Top 100"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT * FROM prospects 
    WHERE top_100_rank IS NOT NULL 
    ORDER BY top_100_rank ASC
    """)
    
    rows = cursor.fetchall()
    prospects = [dict(row) for row in rows]
    conn.close()
    
    return prospects


def get_all_prospects(limit: Optional[int] = None) -> List[Dict]:
    """Get all prospects"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM prospects ORDER BY team, system_rank ASC"
    params = []
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    prospects = [dict(row) for row in rows]
    conn.close()
    
    return prospects


def search_prospects(name: str) -> List[Dict]:
    """Search prospects by name"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT * FROM prospects 
    WHERE player_name LIKE ? 
    ORDER BY team, system_rank ASC
    """, (f"%{name}%",))
    
    rows = cursor.fetchall()
    prospects = [dict(row) for row in rows]
    conn.close()
    
    return prospects


def calculate_composite_value() -> int:
    """
    Calculate composite value scores for all prospects
    
    Value calculation:
    - System rank: Inverted (rank 1 = 30 points, rank 30 = 1 point)
    - Top 100: +50 points, scaled by position (higher rank = more points)
    - Level bonus: AAA=10, AA=8, A+=6, A=4, R=2
    - Age factor: Younger gets slight bonus (under 22 = +2 points)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Add column if it doesn't exist
    cursor.execute("""
    ALTER TABLE prospects ADD COLUMN composite_value INTEGER
    """)
    conn.commit()
    conn.close()
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all prospects
    cursor.execute("SELECT * FROM prospects")
    rows = cursor.fetchall()
    
    for row in rows:
        prospect = dict(row)
        value = 0
        
        # System rank (inverted: rank 1 = 30 points, rank 30 = 1 point)
        system_rank = prospect['system_rank'] or 31
        value += max(1, 31 - system_rank)
        
        # Top 100 bonus
        if prospect['top_100_rank']:
            top_100 = prospect['top_100_rank']
            # Higher rank gets more points (rank 1 = 50, rank 100 = 1)
            value += max(1, 51 - top_100)
        
        # Level bonus
        level = prospect.get('level', '').upper()
        level_bonus = {
            'MLB': 20,
            'AAA': 10,
            'AA': 8,
            'A+': 6,
            'HIGH-A': 6,
            'A': 4,
            'LOW-A': 3,
            'ROOKIE': 2,
            'R': 2
        }
        value += level_bonus.get(level, 0)
        
        # Age factor (younger is better)
        if prospect['age'] and prospect['age'] < 22:
            value += 2
        
        # Update composite value
        cursor.execute("""
        UPDATE prospects 
        SET composite_value = ? 
        WHERE id = ?
        """, (value, prospect['id']))
    
    conn.commit()
    conn.close()
    
    print("Calculated composite value scores for all prospects")


def export_prospects_to_json(output_path: str = "backend/output/prospects_export.json") -> None:
    """Export all prospects to JSON file"""
    prospects = get_all_prospects()
    
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output, 'w') as f:
        json.dump(prospects, f, indent=2, default=str)
    
    print(f"Exported {len(prospects)} prospects to {output_path}")


def load_from_cache(cache_dir: str = "data/prospects_cache") -> Dict[str, List[Dict]]:
    """Load prospect data from cache files"""
    cache_path = Path(cache_dir)
    prospects_data = {}
    
    # Try to load the combined file first
    combined_files = list(cache_path.glob("all_prospects_*.json"))
    if combined_files:
        latest_file = max(combined_files, key=lambda p: p.stat().st_mtime)
        with open(latest_file, 'r') as f:
            prospects_data = json.load(f)
        print(f"Loaded prospects from {latest_file.name}")
        return prospects_data
    
    # Otherwise load individual team files
    for team_file in cache_path.glob("*.json"):
        if "all_prospects" not in team_file.name:
            team_key = team_file.stem.split('_')[0]
            with open(team_file, 'r') as f:
                prospects_data[team_key] = json.load(f)
    
    print(f"Loaded {len(prospects_data)} teams from cache")
    return prospects_data


if __name__ == "__main__":
    # Create table
    create_prospects_table()
    
    # Load from cache and ingest
    prospects_data = load_from_cache()
    
    if prospects_data:
        ingest_prospects(prospects_data)
        calculate_composite_value()
        export_prospects_to_json()
    else:
        print("No prospect data found in cache. Run the scraper first.")
