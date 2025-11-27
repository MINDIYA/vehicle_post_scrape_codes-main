import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import random
import csv
import undetected_chromedriver as uc

def setup_undetected_driver():
    """Setup undetected Chrome driver with stealth settings"""
    # Create options
    options = uc.ChromeOptions()
    
    # Basic settings
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # Randomize window size
    window_sizes = [(1366, 768), (1920, 1080), (1440, 900), (1536, 864)]
    width, height = random.choice(window_sizes)
    options.add_argument(f'--window-size={width},{height}')
    
    # Randomize user agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')
    
    # Add some additional arguments to appear more human-like
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-notifications')
    
    # Create driver
    try:
        driver = uc.Chrome(options=options, version_main=None)
        
        # Execute JavaScript to remove webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Additional stealth JavaScript
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                })
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                })
                window.chrome = {
                    runtime: {}
                }
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({
                        query: () => Promise.resolve({ state: 'granted' })
                    })
                })
            '''
        })
        
        return driver
    except Exception as e:
        print(f"Error creating undetected driver: {e}")
        print("Falling back to standard Chrome driver with anti-detection measures...")
        return setup_standard_driver()

def setup_standard_driver():
    """Fallback to standard Chrome driver with anti-detection measures"""
    options = webdriver.ChromeOptions()
    
    # Anti-detection arguments
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Window size
    options.add_argument('--window-size=1920,1080')
    
    # User agent
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Other settings
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Remove webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def wait_for_cloudflare(driver, max_wait=30):
    """Wait for Cloudflare challenge to complete"""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        page_source = driver.page_source.lower()
        if "attention required" not in page_source and "cf-wrapper" not in page_source and "cloudflare" not in page_source:
            # Check if we have actual content
            if "riyasewana" in page_source or "content" in page_source:
                return True
        time.sleep(1)
    return False

def human_like_delay():
    """Add random human-like delay"""
    delay = random.uniform(2, 5) + random.random()
    time.sleep(delay)

def scrape_with_retry(driver, url, max_retries=3):
    """Try to scrape a URL with retries and Cloudflare handling"""
    for attempt in range(max_retries):
        try:
            # Add random delay before request
            if attempt > 0:
                time.sleep(random.uniform(10, 20))
            
            driver.get(url)
            
            # Wait for Cloudflare challenge if present
            if not wait_for_cloudflare(driver):
                print(f"Cloudflare challenge timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue
            
            # Human-like delay
            human_like_delay()
            
            # Random mouse movements (simulate human behavior)
            try:
                driver.execute_script("""
                    var event = new MouseEvent('mousemove', {
                        view: window,
                        bubbles: true,
                        cancelable: true,
                        clientX: Math.random() * window.innerWidth,
                        clientY: Math.random() * window.innerHeight
                    });
                    document.dispatchEvent(event);
                """)
            except:
                pass
            
            # Scroll a bit randomly
            try:
                scroll_amount = random.randint(100, 300)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1))
                driver.execute_script(f"window.scrollBy(0, -{scroll_amount//2});")
            except:
                pass
            
            # Wait a bit more
            time.sleep(random.uniform(2, 4))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Check if we got the actual content
            content_div = soup.find('div', id='content')
            if content_div:
                return soup
            else:
                print(f"No content found on attempt {attempt + 1}")
                
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
    
    return None

def main():
    makes = ['nissan', 'suzuki', 'micro', 'mitsubishi', 'mahindra', 'mazda', 
             'daihatsu', 'hyundai', 'kia', 'bmw', 'perodua', 'tata']
    types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
    
    # For testing, start with fewer items
    # makes = ['suzuki']
    # types = ['cars']
    
    # Setup driver
    print("Setting up Chrome driver with anti-detection measures...")
    driver = setup_undetected_driver()
    
    if driver is None:
        print("Failed to create driver. Exiting...")
        return
    
    try:
        # First, visit the main page to establish cookies
        print("Establishing initial connection to riyasewana.com...")
        driver.get('https://riyasewana.com')
        
        # Wait for page to load and handle any Cloudflare challenge
        if not wait_for_cloudflare(driver, max_wait=60):
            print("Warning: Initial connection might have been blocked")
        
        human_like_delay()
        
        # Accept cookies if present
        try:
            cookie_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'OK')]")
            cookie_button.click()
            time.sleep(1)
        except:
            pass
        
        print("Starting scraping process...")
        
        total_requests = len(makes) * len(types) * 10
        
        # Open the CSV file for writing
        with open('hrefs_list.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['DATE', 'MAKE', 'TYPE', 'URL', 'PAGE'])
            
            with tqdm(total=total_requests, desc="Scraping Progress") as pbar:
                for make in makes:
                    for type_name in types:
                        page = 1
                        isHaveNextPage = True
                        consecutive_failures = 0
                        
                        while isHaveNextPage and page <= 20:  # Limit pages to prevent infinite loops
                            if page == 1:
                                target_url = f'https://riyasewana.com/search/{type_name}/{make}'
                            else:
                                target_url = f'https://riyasewana.com/search/{type_name}/{make}?page={page}'
                            
                            print(f"\nScraping: {make} - {type_name} - Page {page}")
                            
                            # Try to scrape the page
                            soup = scrape_with_retry(driver, target_url)
                            
                            if soup is None:
                                consecutive_failures += 1
                                if consecutive_failures >= 3:
                                    print(f"Too many failures for {make}:{type_name}. Moving to next combination.")
                                    isHaveNextPage = False
                                    break
                                else:
                                    print(f"Failed to scrape {target_url}. Waiting before retry...")
                                    time.sleep(random.uniform(30, 60))
                                    continue
                            
                            consecutive_failures = 0
                            
                            # Process the content
                            content_div = soup.find('div', id='content')
                            
                            if content_div is None:
                                print(f"No content div found for {make}:{type_name}:{page}")
                                isHaveNextPage = False
                                break
                            
                            # Check pagination
                            pagination = soup.find('div', class_='pagination')
                            
                            if pagination:
                                current_elem = pagination.find('a', class_="current")
                                if current_elem:
                                    try:
                                        current_page = int(current_elem.text.strip())
                                    except:
                                        current_page = 1
                                else:
                                    current_page = 1
                            else:
                                current_page = 1
                            
                            if current_page != page:
                                print(f"Page mismatch for {make}:{type_name}:{page} (got page {current_page})")
                                isHaveNextPage = False
                                break
                            
                            # Find listings
                            ul_tag = content_div.find('ul')
                            
                            if ul_tag is None:
                                print(f"No listings found for {make}:{type_name}:{page}")
                                isHaveNextPage = False
                                break
                            
                            li_tags = ul_tag.find_all('li', class_="item round")
                            
                            if len(li_tags) == 0:
                                print(f"No items found for {make}:{type_name}:{page}")
                                isHaveNextPage = False
                                break
                            
                            print(f"Found {len(li_tags)} listings on page {page}")
                            
                            # Process each listing
                            items_written = 0
                            for li in li_tags:
                                h2_tag = li.find('h2')
                                date_div = li.find('div', class_='boxintxt s')
                                date_text = date_div.text.strip() if date_div else ""
                                
                                # Check date
                                try:
                                    if date_text:
                                        date_obj = pd.to_datetime(date_text)
                                        if date_obj < pd.Timestamp('2024-01-01'):  # Updated cutoff date
                                            print(f"Reached date limit: {date_text}")
                                            isHaveNextPage = False
                                            break
                                except (ValueError, TypeError) as e:
                                    pass  # Continue if date parsing fails
                                
                                # Extract URL
                                if h2_tag:
                                    a_tag = h2_tag.find('a')
                                    if a_tag and 'href' in a_tag.attrs:
                                        writer.writerow([date_text, make, type_name, a_tag['href'], page])
                                        items_written += 1
                            
                            print(f"Wrote {items_written} items to CSV")
                            
                            # Add random delay between pages (human-like behavior)
                            delay = random.uniform(5, 15)
                            print(f"Waiting {delay:.1f} seconds before next page...")
                            time.sleep(delay)
                            
                            page += 1
                            pbar.update(1)
                        
                        # Reset page counter for next combination
                        page = 1
                        
                        # Longer delay between different make/type combinations
                        if isHaveNextPage:
                            time.sleep(random.uniform(10, 20))
    
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Quit the driver
        print("\nClosing browser...")
        driver.quit()
        print("Scraping completed. Results saved to hrefs_list.csv")

if __name__ == "__main__":
    main()