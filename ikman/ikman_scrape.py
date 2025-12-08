#!/usr/bin/env python3
"""
IKMAN.LK SCRAPER - TURBO EDITION (UNLOCKED)
- ENGINE: CloudScraper (Bypasses basic Cloudflare checks).
- SPEED: Sleep timers removed, concurrency maximized.
- LOGIC: Full regex sweep for Specs (Fuel, Trans), Location, and YOM.
"""

import time
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
import cloudscraper

# ==========================================
# ⚙️ TURBO CONFIGURATION
# ==========================================
# CAUTION: High thread counts may trigger IP bans.
SEARCH_WORKERS = 20      # Scans pages quickly
DETAIL_WORKERS = 100     # Extracts data in parallel (High I/O)

MAKES = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 
         'daihatsu', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'mercedes-benz', 'land-rover']
TYPES = ['cars', 'vans', 'suvs', 'motorbikes', 'heavy-duty'] 

MAX_PAGES_PER_COMBO = 160
DAYS_TO_KEEP = 30
BATCH_SIZE = 50  # Increased batch size for disk I/O optimization

print("="*60)
print(f"🚀 IKMAN.LK TURBO SCRAPER")
print(f"⚡ Search Threads: {SEARCH_WORKERS} | 📥 Extractor Threads: {DETAIL_WORKERS}")
print("="*60)

# Increased queue size to handle the flood of incoming links
ad_queue = queue.Queue(maxsize=10000)
stop_event = threading.Event()
stats = {'found': 0, 'saved': 0, 'skipped_date': 0}
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

def get_scraper():
    """Creates a configured CloudScraper instance."""
    return cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )

def fetch_url(scraper, url, retries=2):
    """
    TURBO FETCHER: Minimal sleep, fast timeouts.
    """
    for _ in range(retries):
        try:
            # Timeout reduced to 10s to fail fast on stuck connections
            r = scraper.get(url, timeout=10)
            
            if r.status_code == 200:
                return r.text
            elif r.status_code == 404:
                return None
            elif r.status_code == 429:
                # 429 = Too Many Requests. We MUST pause briefly or we die.
                time.sleep(5)
            # If 403, we retry immediately (sometimes it's just a handshake fail)
        except Exception:
            pass 
    return None

def parse_ikman_date(date_str):
    today = datetime.now()
    date_str = date_str.lower().strip()
    try:
        if 'minute' in date_str or 'hour' in date_str or 'now' in date_str:
            return today.strftime("%Y-%m-%d")
        elif 'yesterday' in date_str:
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            parts = date_str.split()
            day = int(parts[0])
            month_str = parts[1][:3]
            year = int(parts[2]) if len(parts) > 2 else today.year
            month_map = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6, 
                         'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
            return datetime(year, month_map[month_str], day).strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

# ---------------------------------------------------------
# WORKER 1: HARVESTER (SEARCH PAGES)
# ---------------------------------------------------------
def harvest_task(make, v_type, page_num, cutoff_date):
    scraper = get_scraper()
    
    url = f"https://ikman.lk/en/ads/sri-lanka/{v_type}/{make}"
    if page_num > 1: url += f"?page={page_num}"
    
    html = fetch_url(scraper, url)
    if not html: return 0

    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('li', class_=re.compile(r'(normal|top-ad)--'))
    
    local_count = 0
    unique_check = set()

    for item in items:
        try:
            a_tag = item.find('a', href=True)
            if not a_tag: continue
            
            href = f"https://ikman.lk{a_tag['href']}"
            if href in unique_check: continue
            unique_check.add(href)

            title_tag = item.find('h2', class_=re.compile(r'heading--'))
            title = title_tag.get_text(strip=True) if title_tag else "Unknown"

            price = "0"
            price_tag = item.find('div', class_=re.compile(r'price--'))
            if price_tag:
                price_match = re.search(r'Rs\s*([\d,]+)', price_tag.get_text(strip=True))
                if price_match: price = price_match.group(1).replace(',', '')

            item_data = {'url': href, 'date': "Check_Page", 'make': make, 'type': v_type, 'title': title, 'price': price}
            ad_queue.put(item_data)
            local_count += 1
        except: continue

    with stats_lock: stats['found'] += local_count
    return local_count

# ---------------------------------------------------------
# WORKER 2: EXTRACTOR (DETAIL PAGES)
# ---------------------------------------------------------
def extractor_worker(basic_writer, detail_writer, cutoff_date):
    scraper = get_scraper()

    # --- COMPILED PATTERNS ---
    re_fuel = re.compile(r'\b(Petrol|Diesel|Hybrid|Electric|CNG)\b', re.IGNORECASE)
    re_trans = re.compile(r'\b(Automatic|Manual|Tiptronic|Other transmission)\b', re.IGNORECASE)
    re_phone = re.compile(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}')

    while not stop_event.is_set() or not ad_queue.empty():
        try:
            item = ad_queue.get(timeout=3)
        except queue.Empty: continue

        try:
            html = fetch_url(scraper, item['url'])
            if not html: continue

            soup = BeautifulSoup(html, "html.parser")
            full_text = soup.get_text(" ", strip=True)

            # --- 1. DATE ---
            final_date = item['date']
            if final_date == "Check_Page":
                date_div = soup.find(string=re.compile(r'(Posted on|Updated on)'))
                if date_div:
                    raw_d = date_div.find_parent().get_text(strip=True).replace("Posted on", "").replace("Updated on", "").split(",")[0].strip()
                    parsed_d = parse_ikman_date(raw_d)
                    try:
                        if datetime.strptime(parsed_d, "%Y-%m-%d") < cutoff_date:
                            with stats_lock: stats['skipped_date'] += 1
                            continue
                        final_date = parsed_d
                    except: pass
                else:
                    final_date = datetime.now().strftime("%Y-%m-%d")

            details = {
                'YOM': '', 'Transmission': '', 'Fuel': '', 
                'Engine': '', 'Mileage': '', 'Contact': 'Unknown', 
                'Location': '', 'Description': ''
            }

            # --- 2. KEYWORD SWEEP (The Fix) ---
            m_fuel = re_fuel.search(full_text)
            if m_fuel: details['Fuel'] = m_fuel.group(1).title()

            m_trans = re_trans.search(full_text)
            if m_trans: details['Transmission'] = m_trans.group(1).title()

            # --- 3. TEXT STREAM SCANNER (For YOM, Engine, Mileage) ---
            lines = [line.strip() for line in soup.get_text("\n").split("\n") if line.strip()]
            for i, line in enumerate(lines):
                line_lower = line.lower()
                if i + 1 < len(lines):
                    next_line = lines[i+1]
                    if 'model year' in line_lower and not details['YOM']: 
                        details['YOM'] = next_line
                    elif 'engine capacity' in line_lower and not details['Engine']: 
                        details['Engine'] = next_line
                    elif 'mileage' in line_lower and not details['Mileage']: 
                        details['Mileage'] = next_line

            # --- 4. LOCATION FIX ---
            loc_link = soup.find('a', class_=re.compile(r'subtitle-location-link'))
            if loc_link:
                details['Location'] = loc_link.get_text(strip=True)
            else:
                parent_loc = soup.find('a', class_=re.compile(r'subtitle-parentlocation-link'))
                if parent_loc:
                    txt = parent_loc.get_text(strip=True)
                    if " in " in txt: details['Location'] = txt.split(" in ")[-1].strip()

            # --- 5. PRICE & CONTACT ---
            if item['price'] == "0":
                for line in lines[:20]:
                    if 'Rs' in line:
                        p_match = re.search(r'Rs\s*([\d,]+)', line)
                        if p_match: 
                            item['price'] = p_match.group(1).replace(',', '')
                            break

            phones = re_phone.findall(full_text)
            if phones:
                clean_phones = list(set([p.replace('-', '').replace(' ', '') for p in phones]))
                details['Contact'] = " / ".join(clean_phones)

            desc_container = soup.find('div', class_=re.compile(r'description--'))
            if desc_container:
                details['Description'] = desc_container.get_text(strip=True).replace("Show more", "")[:500]

            # Save
            row_basic = {'Date': final_date, 'Make': item['make'], 'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 'Price': item['price']}
            row_detailed = {'Date': final_date, 'Make': item['make'], 'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 'Price': item['price'], 'Transmission': details['Transmission'], 'Fuel': details['Fuel'], 'Engine': details['Engine'], 'Mileage': details['Mileage'], 'Location': details['Location'], 'Contact': details['Contact'], 'URL': item['url']}

            basic_writer.add_row(row_basic)
            detail_writer.add_row(row_detailed)
            with stats_lock: stats['saved'] += 1

        except: pass
        finally: ad_queue.task_done()

def main():
    cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    folder = "ikman_specs_data"
    if not os.path.exists(folder): os.makedirs(folder)
    
    ts = time.strftime('%Y-%m-%d_%H-%M')
    basic_csv = f"{folder}/IKMAN_BASIC_{ts}.csv"
    detail_csv = f"{folder}/IKMAN_DETAILED_{ts}.csv"
    
    basic_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price']
    detail_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL']

    bw = BatchWriter(basic_csv, basic_fields)
    dw = BatchWriter(detail_csv, detail_fields)

    print("🚀 Starting Ikman Turbo Scraper...")
    
    # Start Extractor Workers First
    ex_pool = ThreadPoolExecutor(max_workers=DETAIL_WORKERS)
    for _ in range(DETAIL_WORKERS):
        ex_pool.submit(extractor_worker, bw, dw, cutoff)

    # Generate Search Tasks
    tasks = []
    for make in MAKES:
        for v_type in TYPES:
            for page in range(1, MAX_PAGES_PER_COMBO + 1):
                tasks.append((make, v_type, page))
    random.shuffle(tasks)
    
    # Start Search Workers
    with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as s_pool:
        futures = []
        for (m, t, p) in tasks:
            futures.append(s_pool.submit(harvest_task, m, t, p, cutoff))
        
        # Mininterval slows down the console print to save CPU resources
        with tqdm(total=len(futures), desc="Crawling", unit="pg", mininterval=1.0) as pbar:
            for _ in as_completed(futures):
                pbar.update(1)
                pbar.set_postfix({"Found": stats['found'], "Saved": stats['saved'], "Old": stats['skipped_date']})

    ad_queue.join()
    stop_event.set()
    ex_pool.shutdown(wait=True)
    bw.flush()
    dw.flush()
    print("\n✅ DONE")

if __name__ == "__main__":
    main()