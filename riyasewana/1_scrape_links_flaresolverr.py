import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import random
import csv
import json
from datetime import datetime, timedelta


class FlareSolverrScraper:
    def __init__(self, flaresolverr_url="http://localhost:8191/v1"):
        """Initialize the FlareSolverr scraper"""
        self.flaresolverr_url = flaresolverr_url
        self.headers = {"Content-Type": "application/json"}
        self.session_id = None
        
        # Test connection to FlareSolverr
        if not self.test_connection():
            raise Exception("Could not connect to FlareSolverr. Make sure it's running on port 8191")
    
    def test_connection(self):
        """Test if FlareSolverr is running"""
        try:
            response = requests.get("http://localhost:8191/health", timeout=5)
            if response.status_code == 200:
                print("✓ FlareSolverr is running and healthy")
                return True
        except:
            print("✗ FlareSolverr is not running. Please start it first:")
            print("  docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest")
            return False
    
    def create_session(self):
        """Create a FlareSolverr session for cookie persistence"""
        data = {
            "cmd": "sessions.create",
            "session": f"session_{random.randint(1000, 9999)}"
        }
        
        try:
            response = requests.post(self.flaresolverr_url, headers=self.headers, json=data)
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'ok':
                    self.session_id = result.get('session')
                    print(f"✓ Created session: {self.session_id}")
                    return True
        except Exception as e:
            print(f"✗ Failed to create session: {e}")
        return False
    
    def destroy_session(self):
        """Destroy the FlareSolverr session"""
        if self.session_id:
            data = {
                "cmd": "sessions.destroy",
                "session": self.session_id
            }
            try:
                requests.post(self.flaresolverr_url, headers=self.headers, json=data)
                print(f"✓ Destroyed session: {self.session_id}")
            except:
                pass
    
    def fetch_page(self, url, max_retries=3):
        """Fetch a page using FlareSolverr"""
        for attempt in range(max_retries):
            try:
                # Prepare request data
                data = {
                    "cmd": "request.get",
                    "url": url,
                    "maxTimeout": 60000  # 60 seconds timeout
                }
                
                # Add session if available
                if self.session_id:
                    data["session"] = self.session_id
                
                # Send request to FlareSolverr
                response = requests.post(
                    self.flaresolverr_url, 
                    headers=self.headers, 
                    json=data,
                    timeout=70  # Slightly higher than maxTimeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result.get('status') == 'ok':
                        # Extract HTML content
                        html_content = result.get('solution', {}).get('response', '')
                        if html_content:
                            return html_content
                        else:
                            print(f"  No content returned for {url}")
                    else:
                        print(f"  FlareSolverr error: {result.get('message', 'Unknown error')}")
                else:
                    print(f"  HTTP error {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"  Timeout on attempt {attempt + 1}")
            except Exception as e:
                print(f"  Error on attempt {attempt + 1}: {e}")
            
            if attempt < max_retries - 1:
                wait_time = random.uniform(5, 10)
                print(f"  Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
        
        return None
    
    def parse_page(self, html_content):
        """Parse the HTML content and extract vehicle listings"""
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        # Find content div
        content_div = soup.find('div', id='content')
        if not content_div:
            return results, None
        
        # Check pagination for current page
        pagination = soup.find('div', class_='pagination')
        current_page = 1
        if pagination:
            current_elem = pagination.find('a', class_='current')
            if current_elem:
                try:
                    current_page = int(current_elem.text.strip())
                except:
                    pass
        
        # Find listings
        ul_tag = content_div.find('ul')
        if not ul_tag:
            return results, current_page
        
        li_tags = ul_tag.find_all('li', class_='item round')
        
        for li in li_tags:
            item_data = {}
            
            # Extract date
            date_div = li.find('div', class_='boxintxt s')
            item_data['date'] = date_div.text.strip() if date_div else ""
            
            # Extract URL
            h2_tag = li.find('h2')
            if h2_tag:
                a_tag = h2_tag.find('a')
                if a_tag and 'href' in a_tag.attrs:
                    item_data['url'] = a_tag['href']
                    results.append(item_data)
        
        return results, current_page

def main():
    """Main scraping function"""
    makes = ['nissan', 'suzuki', 'micro', 'mitsubishi', 'mahindra', 'mazda', 
             'daihatsu', 'hyundai', 'kia', 'bmw', 'perodua', 'tata']
    types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
    
    # For testing - uncomment to test with fewer items
    # makes = ['suzuki']
    # types = ['cars']
    
    print("\n" + "="*60)
    print("RIYASEWANA SCRAPER WITH FLARESOLVERR")
    print("="*60)
    print("\nMake sure FlareSolverr is running:")
    print("docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest")
    print("="*60 + "\n")
    
    # Initialize scraper
    try:
        scraper = FlareSolverrScraper()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return
    
    # Create session for cookie persistence
    if not scraper.create_session():
        print("✗ Failed to create session, continuing without session")
    
    # Date cutoff
    date_cutoff = pd.Timestamp('2025-11-01')


    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')  # e.g., '2025-11-20_13-52'
    path = f'riyasewana_hrefs_list_flaresolverr_{timestamp}.csv'
    
    try:
        with open(path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['DATE', 'MAKE', 'TYPE', 'URL', 'PAGE'])
            
            total_combinations = len(makes) * len(types)
            combination_count = 0
            total_items_scraped = 0
            
            print(f"\nStarting to scrape {total_combinations} combinations...\n")
            
            for make in makes:
                for type_name in types:
                    combination_count += 1
                    print(f"\n[{combination_count}/{total_combinations}] Processing: {make} - {type_name}")
                    print("-" * 40)
                    
                    page = 1
                    consecutive_failures = 0
                    combination_items = 0
                    stop_pagination = False
                    
                    while page <= 20 and not stop_pagination:  # Max 20 pages per combination
                        # Build URL
                        if page == 1:
                            url = f'https://riyasewana.com/search/{type_name}/{make}'
                        else:
                            url = f'https://riyasewana.com/search/{type_name}/{make}?page={page}'
                        
                        print(f"\n  Page {page}: {url}")
                        print("  Fetching through FlareSolverr...", end=" ")
                        
                        # Fetch page
                        html_content = scraper.fetch_page(url)
                        
                        if html_content is None:
                            consecutive_failures += 1
                            print(f"Failed (#{consecutive_failures})")
                            
                            if consecutive_failures >= 3:
                                print(f"  ✗ Too many failures for {make} - {type_name}")
                                break
                            
                            # Longer wait after failures
                            wait_time = random.uniform(10, 20)
                            print(f"  Waiting {wait_time:.1f} seconds...")
                            time.sleep(wait_time)
                            continue
                        
                        print("Success!")
                        consecutive_failures = 0
                        
                        # Parse page
                        items, current_page = scraper.parse_page(html_content)
                        
                        # Check if we got the right page
                        if current_page != page:
                            print(f"  ✗ Page mismatch: expected {page}, got {current_page}")
                            stop_pagination = True
                            continue
                        
                        if not items:
                            print(f"  No items found on page {page}")
                            stop_pagination = True
                            continue
                        
                        print(f"  ✓ Found {len(items)} items")
                        
                        # Process items
                        items_written = 0
                        for item in items:
                            # Check date cutoff
                            try:
                                if item['date']:
                                    date_obj = pd.to_datetime(item['date'])
                                    if date_obj < date_cutoff:
                                        print(f"  ⚠ Reached date cutoff: {item['date']}")
                                        stop_pagination = True
                                        break
                            except:
                                pass  # Continue if date parsing fails
                            
                            # Write to CSV
                            if 'url' in item:
                                writer.writerow([
                                    item['date'],
                                    make,
                                    type_name,
                                    item['url'],
                                    page
                                ])
                                items_written += 1
                        
                        combination_items += items_written
                        total_items_scraped += items_written
                        print(f"  ✓ Saved {items_written} items (Combination total: {combination_items})")
                        
                        if stop_pagination:
                            break
                        
                        # Move to next page
                        page += 1
                        
                        # Random delay between pages
                        delay = random.uniform(3, 8)
                        print(f"  Waiting {delay:.1f} seconds before next page...")
                        time.sleep(delay)
                    
                    print(f"  ✓ Completed {make} - {type_name}: {combination_items} items scraped")
                    
                    # Delay between combinations
                    if combination_count < total_combinations:
                        delay = random.uniform(5, 15)
                        print(f"\n  Waiting {delay:.1f} seconds before next combination...")
                        time.sleep(delay)
            
            print("\n" + "="*60)
            print(f"✓ SCRAPING COMPLETED SUCCESSFULLY!")
            print(f"  Total items scraped: {total_items_scraped}")
            print(f"  Results saved to: {path}")
            print("="*60)
    
    except KeyboardInterrupt:
        print("\n\n⚠ Scraping interrupted by user")
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
    finally:
        # Clean up session
        scraper.destroy_session()
        print("\n✓ Cleanup completed")

if __name__ == "__main__":
    main()
