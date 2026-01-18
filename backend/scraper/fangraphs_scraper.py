import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- 1. CONFIGURATION & UTILS ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.fangraphs.com/',
}

# --- 2. "RALPH WIGGUM" MODULE ---
# Simulates random fingerprint profiles to evade detection
class RalphWiggumProfile:
    def __init__(self):
        self.platforms = ['Win32', 'Win64', 'MacIntel', 'MacPPC', 'Linux x86_64']
        self.browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
        self.canvas_hashes = self._generate_random_strings()
        
    def _generate_random_strings(self):
        # Generates random-looking canvas fingerprints
        return "".join([chr(random.randint(97, 122)) for _ in range(random.randint(20, 30))])

    def get_random_headers(self):
        return {
            'User-Agent': self._get_random_user_agent(),
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Not A;Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def _get_random_user_agent(self):
        templates = [
            'Mozilla/5.0 ({0}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{1}.0.0.0 Safari/537.36',
            'Mozilla/5.0 ({0}); rv:2.0) Gecko/20100101 Firefox/{1}.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
        ]
        os_name = random.choice(['Windows NT 10.0; Win64; x64', 'Macintosh; Intel Mac OS X 10_15_7', 'X11; Linux x86_64'])
        browser = random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        version = f"{random.randint(100, 125)}.0.0.0"
        
        return templates[random.randint(0, 2)].format(os_name, browser, version)

# --- 3. "BOTASAURUS" MODULE ---
# Manages the browser lifecycle with "headless" simulation logic
class BotasaurusStealth:
    def __init__(self):
        self.driver = None
        self.ralph = RalphWiggumProfile()

    def start_session(self):
        """
        Initializes the Selenium WebDriver with stealth configurations.
        Mimics starting a real browser session.
        """
        options = webdriver.ChromeOptions()
        
        # Anti-detection: Arguments
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--exclude-switches')
        options.add_argument('--disable-extensions')
        
        # Window Randomization (Ralph Wiggum style)
        width = random.randint(1366, 1920)
        height = random.randint(768, 1080)
        options.add_argument(f'--window-size={width}x{height}')
        
        # User Agent Spoofing
        custom_ua = self.ralph.get_random_headers()['User-Agent']
        options.add_argument(f'user-agent={custom_ua}')

        # Run Headless (standard for server scraping)
        options.add_argument('--headless=new') # New style
        # options.add_argument('--headless') # Old style

        # Initialize Service
        service = Service(ChromeDriverManager().install())

        self.driver = webdriver.Chrome(service=service, options=options)
        
        # Set Page Load Timeout
        self.driver.set_page_load_timeout(30)
        
        # Inject "Human-like" scripts if necessary (advanced)
        # self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
        # self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => []});")
        
        print(f"[Botasaurus] Session Started. Profile: {width}x{height}, UA: {custom_ua[:20]}...")

    def close_session(self):
        if self.driver:
            self.driver.quit()
            print("[Botasaurus] Session Closed.")

# --- 4. FANGRAPHS SCRAPER LOGIC ---
class FangraphsScraper:
    def __init__(self):
        self.bot = BotasaurusStealth()
        self.base_url = "https://www.fangraphs.com"
        self.data = []

    def get_player_stats(self, player_name):
        """
        Scrapes standard batting/pitching stats for a given player.
        """
        search_url = f"{self.base_url}/search.aspx?search={player_name}"
        
        print(f"[*] Navigating to Fangraphs search: {search_url}")
        self.bot.start_session()
        
        try:
            self.bot.driver.get(search_url)
            
            # Wait for the table to load
            # Fangraphs tables usually have class 'seasonWAR', 'war_table' or just generic tables
            # We use a generic waiter to ensure the search results are there.
            WebDriverWait(self.bot.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "table"))
            )

            # Extract Data
            # Since Fangraphs DOM is complex (dynamic tables), we will attempt to grab the summary table.
            # NOTE: In a real production environment, we would parse specific rows.
            # For this demo, we will fetch the page source to inspect the structure.
            
            page_source = self.bot.driver.page_source
            
            # Simple parsing logic for demonstration:
            # In a real scenario, we would click the player link from the search results.
            # Assuming we click the first result to go to the player page.
            
            # Find the first link in the table
            try:
                # Usually the first link in the first table row is the player link
                link_element = self.bot.driver.find_element(By.CSS_SELECTOR, "table td a")
                player_url = link_element.get_attribute('href')
                
                print(f"[*] Found Player Page: {player_url}")
                self.bot.driver.get(player_url)
                
                time.sleep(2) # Wait for dynamic content to load
                
                # Fetch Stats (WAR, OPS, etc.)
                # This is a mockup of parsing logic. 
                # Fangraphs uses tables with IDs like 'batting_standard', 'pitching_standard', 'cWAR'
                
                stats = {
                    'player_name': player_name,
                    'url': player_url,
                    'war': self._extract_text_by_regex(r'WAR\s*</th><td\sclass="">([^<]+)'),
                    'ops': self._extract_text_by_regex(r'OPS\s*</th><td\sclass="">([^<]+)'),
                    'wrc_plus': self._extract_text_by_regex(r'wRC\+\s*</th><td\sclass="">([^<]+)'),
                    'source': 'fangraphs',
                    'timestamp': time.time()
                }
                
                # Fallback if regex fails
                if not stats['war']:
                    stats['status'] = "Data structure might have changed or JS blocked."

                self.data.append(stats)
                return stats

            except Exception as e:
                print(f"[!] Error processing player page: {e}")
                return None

        except Exception as e:
            print(f"[!] Error finding player link: {e}")
            return None
    
    def _extract_text_by_regex(self, pattern):
        """
        Helper to extract stat using Regex if standard DOM traversal is too brittle.
        """
        try:
            match = re.search(pattern, self.bot.driver.page_source)
            return match.group(1).strip() if match else None
        except:
            return None

    def run_batch(self, players_list):
        """
        Runs the scraper for a list of players.
        """
        results = []
        for player in players_list:
            print(f"[*] Processing: {player}")
            stat = self.get_player_stats(player)
            if stat:
                results.append(stat)
            
            # Be polite, random sleep to mimic human reading time
            time.sleep(random.uniform(2.0, 5.0)) 
            
        self.bot.close_session()
        return results

# --- 5. MAIN EXECUTION ---
if __name__ == "__main__":
    import re
    
    # Example Usage
    # You can pass a list of players or specify one to test.
    # The bot will rotate UAs and fingerprints automatically.
    
    print("=== Fangraphs Stealth Scraper ===")
    print("Tools: Botasaurus (Selenium), Ralph Wiggum (Random Profile Gen)")
    
    scraper = FangraphsScraper()
    
    # Test Target (e.g., Mike Trout, or a user provided list)
    # Since no specific list was provided in the prompt's context, I'll use a placeholder.
    target_players = ["Mike Trout", "Shohei Ohtani", "Ronald Acuna Jr."]
    
    # If you have a specific list of players to scrape:
    # target_players = ["Player A", "Player B", ...]
    
    print(f"Starting batch scrape for: {target_players}")
    results = scraper.run_batch(target_players)
    
    # Output to JSON
    import json
    output_file = 'backend/data/fangraphs_cache/scraped_stats.json'
    
    # Ensure directory exists
    import os
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=4)
        
    print(f"\n[*] Scraping Complete.")
    print(f"[*] Results saved to: {output_file}")
    print(f"[*] Total records: {len(results)}")
