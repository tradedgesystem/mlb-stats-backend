# MLB Prospect Scraper System

A comprehensive system for scraping, storing, and analyzing MLB minor league prospect rankings using Botasaurus with anti-bot protection.

## Overview

This system extracts prospect data from MLB.com for all 30 MLB teams, capturing the top 30 prospects from each organization's farm system. It includes:

- **Web Scraper**: Botasaurus-based scraper with anti-detection measures
- **Data Storage**: SQLite database with optimized indexing
- **API Endpoints**: FastAPI endpoints for querying prospect data
- **Value Calculation**: Composite scoring system based on multiple factors

## Architecture

### Components

1. **Team URLs** (`backend/scraper/team_urls.py`)
   - Maps all 30 MLB teams to their prospect URLs
   - Includes team metadata (full name, abbreviation, MLB ID)

2. **Prospect Scraper** (`backend/scraper/prospects_scraper.py`)
   - Uses Botasaurus for anti-bot protection
   - Implements delays, retries, and user agent rotation
   - Extracts prospect data using multiple parsing strategies
   - Falls back to JavaScript extraction if HTML parsing fails

3. **Data Ingestion** (`backend/ingest_prospects.py`)
   - Creates and manages SQLite database schema
   - Ingests scraped data with deduplication
   - Calculates composite value scores
   - Provides data export functionality

4. **API Endpoints** (`backend/api.py`)
   - Query prospects by team
   - Search prospects by name
   - Get Top 100 prospects
   - Compare multiple prospects

## Installation

```bash
cd backend
pip install -r requirements.txt
```

## Usage

### 1. Scrape Prospects

Test with a single team first:

```bash
cd backend
python -m scraper.prospects_scraper
```

This will scrape the Yankees prospects and save to cache.

Scrape all 30 teams:

```python
from scraper import scrape_all_teams, save_to_cache

# Scrape all teams
prospects_data = scrape_all_teams()

# Save to cache
save_to_cache(prospects_data)
```

### 2. Ingest Data

Load scraped data into the database:

```bash
python ingest_prospects.py
```

Or programmatically:

```python
from ingest_prospects import (
    create_prospects_table,
    load_from_cache,
    ingest_prospects,
    calculate_composite_value,
    export_prospects_to_json
)

# Create database tables
create_prospects_table()

# Load from cache
prospects_data = load_from_cache()

# Ingest into database
ingest_prospects(prospects_data)

# Calculate value scores
calculate_composite_value()

# Export to JSON
export_prospects_to_json()
```

### 3. Query Data via API

Start the API server:

```bash
uvicorn api:app --reload
```

#### Available Endpoints

**Get all prospects**
```
GET /prospects
```

Optional parameters:
- `team`: Filter by team name (e.g., "New York Yankees")
- `limit`: Limit number of results (1-1000)

**Get prospects for specific team**
```
GET /prospects/team?team=New+York+Yankees
GET /prospects/team?team=New+York+Yankees&limit=10
```

**Get Top 100 prospects**
```
GET /prospects/top100
```

**Search prospects by name**
```
GET /prospects/search?q=John
```

**Compare prospects**
```
GET /prospects/compare?player_names=John+Smith,Jane+Doe
```

## Data Model

### Prospects Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| player_name | TEXT | Prospect name |
| team | TEXT | Full team name |
| team_abbreviation | TEXT | 3-letter team code |
| mlb_id | INTEGER | MLB team ID |
| system_rank | INTEGER | Rank within team system (1-30) |
| top_100_rank | INTEGER | Overall Top 100 rank (if applicable) |
| fv_value | TEXT | Future Value rating (e.g., "60", "55+") |
| position | TEXT | Primary position |
| age | INTEGER | Player age |
| level | TEXT | Current minor league level |
| eta | TEXT | Expected MLB arrival date |
| data_source | TEXT | Source of data |
| last_updated | TEXT | Last update timestamp |
| created_at | TEXT | First entry timestamp |
| composite_value | INTEGER | Calculated value score |

### Composite Value Calculation

The composite value score combines multiple factors:

1. **System Rank**: Inverted scoring (rank 1 = 30 points, rank 30 = 1 point)
2. **Top 100**: Bonus points (rank 1 = 50, rank 100 = 1)
3. **Level Bonus**: AAA=10, AA=8, A+=6, A=4, Rookie=2
4. **Age Factor**: +2 points for players under 22

Higher scores indicate more valuable prospects.

## Anti-Bot Measures

The scraper includes multiple anti-detection features:

1. **Random Delays**: Variable delays between requests (2-8 seconds)
2. **User Agent Rotation**: Multiple user agent strings
3. **Headless Browser**: Runs in background without UI
4. **Anti-Detection Mode**: Botasaurus anti-detect features enabled
5. **Resource Blocking**: Blocks images and WebGL to reduce fingerprinting
6. **Retry Logic**: Automatic retries on failures (up to 3 attempts)
7. **Team-Level Delays**: 5-8 seconds between different teams

## Configuration

Adjust anti-bot settings in `backend/scraper/prospects_scraper.py`:

```python
BASE_DELAY = 2  # Base delay between page loads
DELAY_VARIANCE = 1  # Random variance
TEAM_DELAY = 5  # Delay between teams
TEAM_DELAY_VARIANCE = 3  # Random variance for team delays
MAX_RETRIES = 3  # Maximum retry attempts
```

## Data Sources

### Primary Source: MLB.com

**URL Pattern**: `https://www.mlb.com/{team_slug}/prospects/stats/top-prospects`

**Advantages**:
- Official MLB data
- Consistent format across teams
- Top 30 prospects per team
- Includes Future Value (FV) ratings

**Limitations**:
- May require JavaScript rendering
- Rate limiting possible
- Dynamic HTML structure

### Alternative Sources (Future Enhancements)

1. **MLB Pipeline**
   - Professional scouting grades
   - Comprehensive player info
   - May require subscription

2. **Baseball America**
   - Industry-standard rankings
   - Detailed scouting reports
   - Top 100 lists

3. **FanGraphs**
   - Statistical projections
   - Future Value grades
   - Major and minor league data

4. **Prospects Live**
   - Live prospect rankings
   - Trade value analysis
   - Team system rankings

## Troubleshooting

### Scraper Returns No Data

1. **Check URL**: Verify the team URL is accessible
2. **Inspect Page**: Use browser dev tools to check page structure
3. **Increase Delays**: Slow down scraping if getting blocked
4. **Check Logs**: Review error messages for specific issues

### Database Errors

1. **Table Exists**: If table creation fails, table may already exist
2. **Duplicate Entries**: Uses INSERT OR REPLACE to handle duplicates
3. **Column Errors**: May need to update schema if columns change

### API Errors

1. **404 Not Found**: Prospect doesn't exist in database
2. **400 Bad Request**: Invalid query parameters
3. **500 Server Error**: Database connection issue

## Performance Considerations

### Scraping Speed

- **Single Team**: ~30 seconds (including delays)
- **All 30 Teams**: ~15-20 minutes
- **With Anti-Bot Delays**: ~25-30 minutes

### Database Performance

- **Indexes**: Created on team, system_rank, and top_100_rank
- **Query Speed**: <100ms for typical queries
- **Storage**: ~5MB for full prospect dataset

## Future Enhancements

1. **Multi-Source Aggregation**: Combine data from multiple sources
2. **Automated Updates**: Schedule regular data refreshes
3. **Historical Tracking**: Track prospect movement over time
4. **Machine Learning**: Predict prospect success based on metrics
5. **Visualization**: Dashboard for prospect data
6. **Trade Analysis**: Calculate trade value of prospects
7. **Performance Tracking**: Minor league stats integration
8. **Injury Tracking**: Monitor prospect health status

## Contributing

When adding new features:

1. Test with a single team first
2. Validate data quality
3. Update documentation
4. Consider anti-bot implications
5. Test database queries performance

## License

This project is part of the MLB Stats backend system.

## Support

For issues or questions:
1. Check troubleshooting section
2. Review error logs
3. Test with single team before full scrape
4. Verify network connectivity

## Credits

Built with:
- **Botasaurus**: Anti-bot web scraping framework
- **BeautifulSoup**: HTML parsing
- **SQLite**: Data storage
- **FastAPI**: API framework
- **MLB.com**: Data source
