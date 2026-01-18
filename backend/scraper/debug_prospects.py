"""
Debug script to inspect the HTML structure of MLB prospect pages
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

from .team_urls import get_team_url, TEAMS


def debug_page(team_key: str = "yankees"):
    """Download and save HTML for inspection"""
    url = get_team_url(team_key)
    team_info = TEAMS[team_key]
    
    print(f"Debugging {team_info['full_name']} at {url}")
    
    # Create driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Navigate to page
        print("Loading page...")
        driver.get(url)
        
        # Wait for page to load
        time.sleep(3)
        
        # Get page source
        html = driver.page_source
        
        # Save HTML to file
        filename = f"data/prospects_cache/{team_key}_debug.html"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"Saved HTML to {filename}")
        
        # Parse with BeautifulSoup and try to find prospect-related elements
        soup = BeautifulSoup(html, 'html.parser')
        
        print("\n=== Page Analysis ===")
        print(f"Total elements: {len(soup.find_all())}")
        
        # Look for tables
        tables = soup.find_all('table')
        print(f"Tables found: {len(tables)}")
        
        if tables:
            for i, table in enumerate(tables[:3]):  # Show first 3 tables
                print(f"\nTable {i+1}:")
                rows = table.find_all('tr')
                print(f"  Rows: {len(rows)}")
                if rows:
                    headers = rows[0].find_all(['th', 'td'])
                    print(f"  Headers: {[h.get_text(strip=True) for h in headers[:10]]}")
        
        # Look for common prospect-related classes
        print("\n=== Searching for prospect-related elements ===")
        selectors = [
            'table',
            '.prospect',
            '.player',
            '.rank',
            '[class*="prospect"]',
            '[class*="player"]',
            '[class*="rank"]',
            'tbody tr',
        ]
        
        for selector in selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    print(f"\nSelector '{selector}': {len(elements)} elements found")
                    if len(elements) > 0:
                        first_elem = elements[0]
                        classes = first_elem.get('class', [])
                        print(f"  First element classes: {classes}")
                        text = first_elem.get_text(strip=True)[:100]
                        print(f"  First element text: {text}")
            except Exception as e:
                pass
        
        # Look for any script tags with JSON data
        print("\n=== Looking for JSON data in scripts ===")
        scripts = soup.find_all('script')
        json_scripts = []
        for script in scripts:
            if script.string and ('prospect' in script.string.lower() or 'player' in script.string.lower()):
                json_scripts.append(script)
        
        if json_scripts:
            print(f"Found {len(json_scripts)} scripts with prospect/player data")
            for i, script in enumerate(json_scripts[:2]):
                content = script.string[:500]
                print(f"\nScript {i+1}:")
                print(f"  {content}")
        
    finally:
        driver.quit()
        print("\nDone!")


if __name__ == "__main__":
    debug_page("yankees")
