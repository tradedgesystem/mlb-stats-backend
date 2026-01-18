"""
Test script for MLB prospect scraper
Validates installation and basic functionality
"""

import sys
from pathlib import Path

def check_imports():
    """Check if all required packages are installed"""
    print("Checking required packages...")
    
    required = {
        'botasaurus': 'botasaurus',
        'bs4': 'beautifulsoup4',
        'fastapi': 'fastapi',
    }
    
    missing = []
    for module, package in required.items():
        try:
            __import__(module)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} (not installed)")
            missing.append(package)
    
    if missing:
        print(f"\nMissing packages: {', '.join(missing)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    return True


def check_files():
    """Check if all required files exist"""
    print("\nChecking required files...")
    
    files = [
        'scraper/__init__.py',
        'scraper/team_urls.py',
        'scraper/prospects_scraper.py',
        'ingest_prospects.py',
        'api.py',
    ]
    
    missing = []
    for file in files:
        path = Path(file)
        if path.exists():
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} (not found)")
            missing.append(file)
    
    if missing:
        print(f"\nMissing files: {', '.join(missing)}")
        return False
    
    return True


def check_database():
    """Check if database exists and has correct schema"""
    print("\nChecking database...")
    
    db_path = Path('stats.db')
    if not db_path.exists():
        print("  ℹ Database not found (run ingest_prospects.py to create)")
        return True
    
    print("  ✓ Database exists")
    
    # Check if prospects table exists
    import sqlite3
    try:
        conn = sqlite3.connect('stats.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='prospects'
        """)
        
        if cursor.fetchone():
            print("  ✓ Prospects table exists")
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM prospects")
            count = cursor.fetchone()[0]
            print(f"  ℹ Contains {count} prospects")
        else:
            print("  ℹ Prospects table not found (run ingest_prospects.py)")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Database error: {e}")
        return False


def test_team_urls():
    """Test team URL mappings"""
    print("\nTesting team URL mappings...")
    
    try:
        from scraper.team_urls import TEAMS, get_team_url, get_all_team_urls
        
        print(f"  ✓ Found {len(TEAMS)} teams")
        
        # Test a few URLs
        test_teams = ['yankees', 'dodgers', 'redsox']
        for team in test_teams:
            url = get_team_url(team)
            print(f"  ✓ {team}: {url}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def test_api_endpoints():
    """Test API endpoint functions exist"""
    print("\nTesting API endpoints...")
    
    try:
        # Import API
        import importlib.util
        spec = importlib.util.spec_from_file_location("api", "api.py")
        api_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api_module)
        
        # Check for prospect endpoints
        endpoints = [
            'get_prospects_api',
            'get_top_100_prospects_api',
            'search_prospects_api',
            'get_team_prospects_api',
            'compare_prospects_api',
        ]
        
        for endpoint in endpoints:
            if hasattr(api_module, endpoint):
                print(f"  ✓ {endpoint}")
            else:
                print(f"  ✗ {endpoint} (not found)")
                return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def run_tests():
    """Run all tests"""
    print("=" * 60)
    print("MLB Prospect Scraper - Test Suite")
    print("=" * 60)
    
    results = []
    
    results.append(("Packages", check_imports()))
    results.append(("Files", check_files()))
    results.append(("Database", check_database()))
    results.append(("Team URLs", test_team_urls()))
    results.append(("API Endpoints", test_api_endpoints()))
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        symbol = "✓" if result else "✗"
        print(f"  {symbol} {name:20s} {status}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ All tests passed! System is ready.")
        print("\nNext steps:")
        print("  1. Test scraper with one team: python -m scraper.prospects_scraper")
        print("  2. Scrape all teams: from scraper import scrape_all_teams")
        print("  3. Ingest data: python ingest_prospects.py")
        print("  4. Start API: uvicorn api:app --reload")
        return 0
    else:
        print("\n✗ Some tests failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())
