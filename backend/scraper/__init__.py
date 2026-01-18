"""
MLB Prospect Scraper Package
"""

# Make prospects scraper import optional
try:
    from .prospects_scraper import scrape_team_prospects, save_to_cache
    _has_prospects = True
except ImportError:
    _has_prospects = False
    scrape_team_prospects = None
    save_to_cache = None

from .team_urls import TEAMS, get_team_url, get_all_team_urls

__all__ = [
    'scrape_team_prospects',
    'save_to_cache',
    'TEAMS',
    'get_team_url',
    'get_all_team_urls'
]
