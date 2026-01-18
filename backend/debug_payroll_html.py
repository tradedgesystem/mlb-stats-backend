#!/usr/bin/env python3
"""
Debug script to fetch and save Fangraphs payroll HTML for inspection
"""

from scraper.fangraphs_optout_scraper import FangraphsOptOutScraper
import os
from datetime import datetime

def main():
    print("="*60)
    print("DEBUGGING: Saving HTML for structure analysis")
    print("="*60)
    
    scraper = FangraphsOptOutScraper()
    bot = scraper.bot
    
    # Start browser
    bot.start_session()
    
    # Fetch Yankees page
    url = "https://www.fangraphs.com/roster-resource/payroll/yankees"
    print(f"[*] Fetching: {url}")
    bot.driver.get(url)
    
    # Wait longer for page to load
    import time
    time.sleep(10)
    
    # Save HTML
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "backend/data/fangraphs_cache"
    os.makedirs(output_dir, exist_ok=True)
    html_file = os.path.join(output_dir, f"yankees_payroll_{timestamp}.html")
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(bot.driver.page_source)
    
    print(f"[*] HTML saved to: {html_file}")
    print(f"[*] Page length: {len(bot.driver.page_source)} characters")
    
    # Try to find any table-like structures
    from selenium.webdriver.common.by import By
    tables = bot.driver.find_elements(By.TAG_NAME, "table")
    print(f"[*] Found {len(tables)} <table> elements")
    
    divs = bot.driver.find_elements(By.TAG_NAME, "div")
    print(f"[*] Found {len(divs)} <div> elements")
    
    # Look for any elements with class containing "table"
    table_elements = bot.driver.find_elements(By.XPATH, "//*[contains(@class, 'table')]")
    print(f"[*] Found {len(table_elements)} elements with 'table' in class name")
    
    # Look for any elements with class containing "payroll"
    payroll_elements = bot.driver.find_elements(By.XPATH, "//*[contains(@class, 'payroll')]")
    print(f"[*] Found {len(payroll_elements)} elements with 'payroll' in class name")
    
    # Look for any elements with class containing "roster"
    roster_elements = bot.driver.find_elements(By.XPATH, "//*[contains(@class, 'roster')]")
    print(f"[*] Found {len(roster_elements)} elements with 'roster' in class name")
    
    bot.close_session()
    
    print("\n" + "="*60)
    print("Check the saved HTML file to understand the structure")
    print("="*60)

if __name__ == "__main__":
    main()
