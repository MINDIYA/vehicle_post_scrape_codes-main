import time
import requests
import random
import csv
import re
import threading
import queue
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait

# ==========================================
# 🧠 SMART HARDWARE DETECTION
# ==========================================
cpu_count = os.cpu_count() or 4
# Max safe threads calculation
max_threads = cpu_count * 4
SAFE_CAP = 35 
if max_threads > SAFE_CAP: max_threads = SAFE_CAP

SEARCH_WORKERS = max(2, int(max_threads * 0.2))
DETAIL_WORKERS = max_threads - SEARCH_WORKERS

print("="*60)
print(f"🖥️  DEVICE DETECTED: {cpu_count} CPU Cores")
print(f"🚀 AUTO-SPEED: {SEARCH_WORKERS} Searchers | {DETAIL_WORKERS} Extractors")
print("="*60)

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
BRANDS_TO_SCRAPE = [
    'toyota', 'suzuki', 'nissan', 'honda', 'mitsubishi', 
    'mazda', 'daihatsu', 'kia', 'hyundai', 'micro', 
    'audi', 'bmw', 'mercedes-benz', 'land-rover'
]
PAGES_PER_BRAND = 160     
DAYS_TO_KEEP = 14         
# ==========================================

ad_queue = queue.Queue()
stop_event = threading.Event()

class FlareSolverrClient:
    def __init__(self, flaresolverr_url="http://localhost:8191/v1"):
        self.base_url = flaresolverr_url.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.session = None

    def create_session(self):
        payload = {"cmd": "sessions.create", "session": f"session_{random.randint(1000, 9999)}"}
        try:
            r = requests.post(self.base_url, json=payload, headers=self.headers, timeout=10)
            if r.status_code == 200:
                self.session = r.json().get("session")
                return True
        except: pass
        return False

    def destroy_session(self):
        if self.session:
            try:
                requests.post(self.base_url, json={"cmd": "sessions.destroy", "session": self.session}, headers=self.headers)
            except: pass

    def fetch(self, url, timeout=70):
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000}
        if self.session: payload["session"] = self.session
        try:
            base_sleep = 0.1 if cpu_count > 8 else 0.5
            time.sleep(random.uniform(base_sleep, base_sleep + 0.5))
            r = requests.post(self.base_url, json=payload, headers=self.headers, timeout=timeout)
            if r.status_code == 200:
                return r.json().get("solution", {}).get("response", "")
        except: pass
        return None

# ---------------------------------------------------------
# WORKER 1: THE HARVESTER
# ---------------------------------------------------------
def harvest_task(client, brand, page_num, cutoff_date):
    clean_brand = brand.lower().replace(" ", "-")
    url = f"https://patpat.lk/en/sri-lanka/vehicle/all/{clean_brand}?page={page_num}"
    
    html = client.fetch(url)
    if not html: return

    soup = BeautifulSoup(html, 'html.parser')
    all_links = soup.find_all('a', href=True)
    unique_links = set()

    count = 0
    for link in all_links:
        href = link['href']
        
        if '/ad/vehicle/' in href and clean_brand in href.lower():
            if href in unique_links: continue
            unique_links.add(href)
            
            card_text = ""
            try:
                parent = link.find_parent('div', class_=lambda x: x and ('item' in x or 'row' in x))
                if not parent: parent = link.parent.parent.parent
                card_text = parent.get_text(" ", strip=True)
            except: continue

            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', card_text)
            final_date = "Unknown"
            
            if date_match:
                d_str = date_match.group(1)
                try:
                    if datetime.strptime(d_str, "%Y-%m-%d") < cutoff_date: continue
                    final_date = d_str
                except: continue
            else:
                continue 

            item = {
                'url': href,
                'date': final_date,
                'make': brand
            }
            ad_queue.put(item)
            count += 1
    
    if count > 0:
        print(f"   [Harvest] Found {count} ads on {brand} Pg {page_num}")

# ---------------------------------------------------------
# WORKER 2: THE EXTRACTOR (Writes to 2 Files)
# ---------------------------------------------------------
def extractor_worker(client, basic_writer, detail_writer, lock):
    while not stop_event.is_set() or not ad_queue.empty():
        try:
            item = ad_queue.get(timeout=1) 
        except queue.Empty:
            continue

        url = item['url']
        html = client.fetch(url)
        
        details = {}
        if html:
            soup = BeautifulSoup(html, "html.parser")
            try:
                # Price
                ptag = soup.find("div", class_="price") or soup.find("h3", class_="text-green")
                if ptag: details['Price'] = re.sub(r'[^\d]', '', ptag.get_text(strip=True))

                # Contact
                ctag = soup.find("span", class_="contact-name")
                if ctag: details['Contact'] = ctag.get_text(strip=True)

                # Specs Map
                all_blocks = soup.find_all(['li', 'div', 'p', 'tr'])
                specs_map = {
                    'Year': ['Model Year', 'YOM', 'Year'],
                    'Model': ['Model'],
                    'Transmission': ['Transmission', 'Gear'], 
                    'Fuel': ['Fuel Type', 'Fuel'],
                    'Engine': ['Engine Capacity', 'Engine (cc)'],
                    'Mileage': ['Mileage']
                }
                
                def clean(t): return t.replace(":", "").strip()

                for block in all_blocks:
                    text = block.get_text(" ", strip=True)
                    for key, kws in specs_map.items():
                        if key in details: continue
                        for kw in kws:
                            if kw.lower() in text.lower():
                                parts = text.split(kw)
                                if len(parts) > 1:
                                    val = clean(parts[1])
                                    if len(val) < 50: details[key] = val
                
                # Description
                dtag = soup.find("div", id="description")
                if dtag: details['Description'] = dtag.get_text(" ", strip=True)[:300]

            except: pass

        # 1. Prepare BASIC Data
        basic_row = {
            'Date': item['date'],
            'Make': item['make'],
            'Year': details.get('Year', ''),
            'Model': details.get('Model', ''),
            'Transmission': details.get('Transmission', ''),
            'Price': details.get('Price', ''),
            'URL': item['url']
        }

        # 2. Prepare DETAILED Data (Everything)
        detail_row = {
            'Date': item['date'],
            'Make': item['make'],
            'Year': details.get('Year', ''),
            'Model': details.get('Model', ''),
            'Transmission': details.get('Transmission', ''),
            'Price': details.get('Price', ''),
            'Fuel': details.get('Fuel', ''),
            'Engine': details.get('Engine', ''),
            'Mileage': details.get('Mileage', ''),
            'Contact': details.get('Contact', ''),
            'Description': details.get('Description', ''),
            'URL': item['url']
        }

        # 3. Write to BOTH files Safely
        with lock:
            basic_writer.writerow(basic_row)
            detail_writer.writerow(detail_row)
            
            # Simple progress log
            mod = details.get('Model', 'Car')
            print(f"     -> Saved: {item['make']} {mod}") 
        
        ad_queue.task_done()

def main():
    cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    
    folder = "patpat_final_data"
    if not os.path.exists(folder): os.makedirs(folder)
    
    timestamp = time.strftime('%Y-%m-%d_%H-%M')
    
    # OUTPUT 1: Basic File
    basic_csv = f"{folder}/patpat_BASIC_{timestamp}.csv"
    basic_fields = ['Date', 'Make', 'Year', 'Model', 'Transmission', 'Price', 'URL']

    # OUTPUT 2: Detailed File
    detail_csv = f"{folder}/patpat_DETAILED_{timestamp}.csv"
    detail_fields = ['Date', 'Make', 'Year', 'Model', 'Transmission', 'Price', 'Fuel', 'Engine', 'Mileage', 'Contact', 'Description', 'URL']

    client_harvest = FlareSolverrClient()
    client_harvest.create_session()
    client_extract = FlareSolverrClient() 
    client_extract.create_session()

    file_lock = threading.Lock()

    # OPEN BOTH FILES AT ONCE
    with open(basic_csv, 'w', newline='', encoding='utf-8') as f_basic, \
         open(detail_csv, 'w', newline='', encoding='utf-8') as f_detail:
        
        # Create Writers
        writer_basic = csv.DictWriter(f_basic, fieldnames=basic_fields)
        writer_detail = csv.DictWriter(f_detail, fieldnames=detail_fields)
        
        writer_basic.writeheader()
        writer_detail.writeheader()

        print("🚀 Starting Dual-Extraction Team...")
        
        extraction_executor = ThreadPoolExecutor(max_workers=DETAIL_WORKERS)
        for _ in range(DETAIL_WORKERS):
            # Pass both writers to the worker
            extraction_executor.submit(extractor_worker, client_extract, writer_basic, writer_detail, file_lock)

        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as search_pool:
            for brand in BRANDS_TO_SCRAPE:
                print(f"\n>>> Harvesting Brand: {brand.upper()} <<<")
                futures = []
                for page in range(1, PAGES_PER_BRAND + 1):
                    futures.append(search_pool.submit(harvest_task, client_harvest, brand, page, cutoff))
                
                wait(futures)
                print(f"   (Finished searching {brand})")

        print("\n🛑 Search Complete. Finishing remaining items...")
        ad_queue.join()
        stop_event.set()
        extraction_executor.shutdown(wait=True)

    client_harvest.destroy_session()
    client_extract.destroy_session()
    print("\n" + "="*60)
    print(f"✅ DUAL SCRAPE COMPLETE")
    print(f"📂 Basic File: {basic_csv}")
    print(f"📂 Detailed File: {detail_csv}")
    print("="*60)

if __name__ == "__main__":
    main()