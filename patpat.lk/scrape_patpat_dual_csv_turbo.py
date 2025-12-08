
#!/usr/bin/env python3
"""
FINAL FIXED: patpat_scraper_final_fixed.py
- FIX: 'Register_Year' Unknown -> Added Brute Force Regex Sweep.
- STABILITY: Keeps the 'Stable' threading config to prevent crashes.
- ACCURACY: Scans full page text for missing fields.
"""

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
SEARCH_WORKERS = 3   
DETAIL_WORKERS = 7   

FLARESOLVERR_URL = "http://localhost:8191/v1"

BRANDS_TO_SCRAPE = [
    'toyota', 'suzuki', 'nissan', 'honda', 'mitsubishi', 
    'mazda', 'daihatsu', 'kia', 'hyundai', 'micro', 
    'audi', 'bmw', 'mercedes-benz', 'land-rover', 'mahindra', 'perodua', 'tata'
]
PAGES_PER_BRAND = 160     
DAYS_TO_KEEP = 14
BATCH_SIZE = 10 

print("="*60)
print(f"🚀 FINAL FIXED MODE")
print(f"⚡ Search Threads: {SEARCH_WORKERS} | 📥 Extractor Threads: {DETAIL_WORKERS}")
print("="*60)

ad_queue = queue.Queue(maxsize=1000)
stop_event = threading.Event()
stats = {'found': 0, 'saved': 0, 'skipped': 0}
stats_lock = threading.Lock()

class BatchWriter:
    def __init__(self, filepath, fieldnames):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.buffer = []
        self.lock = threading.Lock()
        with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()

    def add_row(self, row):
        with self.lock:
            self.buffer.append(row)
            if len(self.buffer) >= BATCH_SIZE:
                self.flush_unsafe()

    def flush(self):
        with self.lock:
            if self.buffer: self.flush_unsafe()

    def flush_unsafe(self):
        try:
            with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(self.buffer)
            self.buffer = []
        except: pass

class FlareSolverrClient:
    def __init__(self):
        self.url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.session_id = None

    def create_session(self):
        try:
            sid = f"sess_{random.randint(1000,9999)}_{int(time.time())}"
            payload = {"cmd": "sessions.create", "session": sid}
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=10)
            if r.status_code == 200:
                self.session_id = r.json().get("session")
                return True
        except: pass
        return False

    def destroy_session(self):
        if self.session_id:
            try:
                requests.post(self.url, json={"cmd": "sessions.destroy", "session": self.session_id}, headers=self.headers, timeout=5)
            except: pass

    def fetch(self, url):
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 25000}
        if self.session_id: payload["session"] = self.session_id
        try:
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=30)
            if r.status_code == 200:
                return r.json().get("solution", {}).get("response", "")
        except: pass
        return None

# ---------------------------------------------------------
# WORKER 1: HARVESTER
# ---------------------------------------------------------
def harvest_task(client, brand, page_num, cutoff_date):
    clean_brand = brand.lower().replace(" ", "-")
    url = f"https://patpat.lk/en/sri-lanka/vehicle/all/{clean_brand}?page={page_num}"
    
    html = client.fetch(url)
    if not html: return 0

    soup = BeautifulSoup(html, 'html.parser')
    all_links = soup.find_all('a', href=True)
    unique_links = set()
    local_count = 0

    for link in all_links:
        href = link['href']
        if '/ad/vehicle/' in href and clean_brand in href.lower():
            if href in unique_links: continue
            unique_links.add(href)
            
            card_text = link.parent.get_text(" ", strip=True) + " " + link.get_text(" ", strip=True)

            final_date = "Check_Page"
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', card_text)
            if date_match:
                d_str = date_match.group(1)
                try:
                    if datetime.strptime(d_str, "%Y-%m-%d") < cutoff_date:
                        with stats_lock: stats['skipped'] += 1
                        continue
                    final_date = d_str
                except: pass
            
            search_price = "0"
            price_match = re.search(r'Rs[:\.]?\s*([\d,]+)', card_text)
            if price_match: search_price = price_match.group(1).replace(',', '')

            item = {'url': href, 'date': final_date, 'make': brand, 'title': link.text.strip(), 'price': search_price}
            ad_queue.put(item)
            local_count += 1
    
    with stats_lock: stats['found'] += local_count
    return local_count

# ---------------------------------------------------------
# WORKER 2: EXTRACTOR (FIXED REGISTER YEAR)
# ---------------------------------------------------------
def extractor_worker(basic_writer, detail_writer, cutoff_date):
    client = FlareSolverrClient()
    if not client.create_session(): return

    # Regex for "Brute Force" searching
    re_reg_year = re.compile(r'(?:Register|Registration)\s*(?:Year|Date)?\s*[:\.]?\s*(\d{4})', re.IGNORECASE)

    while not stop_event.is_set() or not ad_queue.empty():
        try:
            item = ad_queue.get(timeout=3)
        except queue.Empty: continue

        try:
            html = client.fetch(item['url'])
            if not html: continue

            soup = BeautifulSoup(html, "html.parser")
            full_text = soup.get_text(" ", strip=True)

            final_date = item['date']
            if final_date == "Check_Page":
                dm = re.search(r'(\d{4}-\d{2}-\d{2})', full_text)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff_date:
                            with stats_lock: stats['skipped'] += 1
                            continue
                        final_date = dm.group(1)
                    except: pass
                else:
                    final_date = datetime.now().strftime("%Y-%m-%d")

            details = {k: "Unknown" for k in ['Model', 'Year', 'Register_Year', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Contact', 'Description']}
            details['Price'] = item['price']

            # --- 1. LIST PARSER (Standard) ---
            lis = soup.find_all('li')
            specs_map = {
                'Year': ['Model Year', 'YOM', 'Year'],
                'Register_Year': ['Register Year', 'Registration Year', 'Registered'], # Added keywords
                'Model': ['Model', 'Model Name'],
                'Transmission': ['Transmission', 'Gear'], 
                'Fuel': ['Fuel Type', 'Fuel'],
                'Engine': ['Engine Capacity', 'Engine'],
                'Mileage': ['Mileage']
            }

            for li in lis:
                txt = li.get_text(" ", strip=True)
                for key, labels in specs_map.items():
                    if details[key] != "Unknown": continue
                    for label in labels:
                        if label.lower() in txt.lower():
                            if key == 'Model' and 'year' in txt.lower(): continue
                            spans = li.find_all('span')
                            if spans: 
                                details[key] = spans[-1].get_text(strip=True)
                            else:
                                # Split fallback
                                parts = txt.split(':')
                                if len(parts) > 1: details[key] = parts[-1].strip()

            # --- 2. BRUTE FORCE FIXES (If Unknown) ---
            
            # FIX: Force scan for Register Year if list parser failed
            if details['Register_Year'] == "Unknown":
                reg_match = re_reg_year.search(full_text)
                if reg_match:
                    details['Register_Year'] = reg_match.group(1)

            # Fallback for Model
            if details['Model'] == "Unknown":
                h1 = soup.find('h1')
                t = h1.get_text(strip=True) if h1 else item['title']
                details['Model'] = t.replace(item['make'], "").strip()

            # Fallback for Contact
            tel = soup.find('a', href=re.compile(r'^tel:'))
            if tel: details['Contact'] = tel['href'].replace('tel:', '')

            d_div = soup.find("div", attrs={"x-show": "active === 1"})
            if d_div: details['Description'] = d_div.get_text(" ", strip=True)[:300]

            # Save
            row_b = {'Date': final_date, 'Make': item['make'], 'Year': details['Year'], 'Model': details['Model'], 'Transmission': details['Transmission'], 'Price': details['Price']}
            row_d = {'Date': final_date, 'Make': item['make'], 'Year': details['Year'], 'Register_Year': details['Register_Year'], 'Model': details['Model'], 'Transmission': details['Transmission'], 'Price': details['Price'], 'Fuel': details['Fuel'], 'Engine': details['Engine'], 'Mileage': details['Mileage'], 'Contact': details['Contact'], 'Description': details['Description'], 'URL': item['url']}
            
            basic_writer.add_row(row_b)
            detail_writer.add_row(row_d)
            with stats_lock: stats['saved'] += 1

        except: pass
        finally: ad_queue.task_done()
    
    client.destroy_session()

def main():
    cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    folder = "patpat_fixed_data"
    if not os.path.exists(folder): os.makedirs(folder)
    
    ts = time.strftime('%Y-%m-%d_%H-%M')
    basic_csv = f"{folder}/BASIC_{ts}.csv"
    detail_csv = f"{folder}/DETAILED_{ts}.csv"

    bw = BatchWriter(basic_csv, ['Date', 'Make', 'Year', 'Model', 'Transmission', 'Price'])
    dw = BatchWriter(detail_csv, ['Date', 'Make', 'Year', 'Register_Year', 'Model', 'Transmission', 'Price', 'Fuel', 'Engine', 'Mileage', 'Contact', 'Description', 'URL'])

    client_h = FlareSolverrClient()
    if not client_h.create_session(): return

    print("🚀 Starting Final Fixed Extraction...")
    
    ex_pool = ThreadPoolExecutor(max_workers=DETAIL_WORKERS)
    for _ in range(DETAIL_WORKERS):
        ex_pool.submit(extractor_worker, bw, dw, cutoff)
        time.sleep(1)

    with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as s_pool:
        futures = []
        for brand in BRANDS_TO_SCRAPE:
            for page in range(1, PAGES_PER_BRAND + 1):
                futures.append(s_pool.submit(harvest_task, client_h, brand, page, cutoff))
        
        with tqdm(total=len(futures), desc="Processing", unit="pg") as pbar:
            for _ in as_completed(futures):
                pbar.update(1)
                pbar.set_postfix({"Found": stats['found'], "Saved": stats['saved'], "Skip": stats['skipped']})

    ad_queue.join()
    stop_event.set()
    ex_pool.shutdown()
    bw.flush()
    dw.flush()
    client_h.destroy_session()
    print("\n✅ DONE")

if __name__ == "__main__":
    main()