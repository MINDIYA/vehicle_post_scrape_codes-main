#!/usr/bin/env python3
"""
SRI LANKA VEHICLE MASTER SCRAPER (FINAL PRODUCTION BUILD)
- ARCHITECTURE: Hybrid (Riyasewana=FlareSolverr, Ikman/Patpat=Requests).
- FINAL FIX: Integrated Patpat's brute-force extraction logic for stability 
             AND added robust Patpat Location extraction.
- OUTPUT: 1 Master CSV + 3 Detail CSVs.
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
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
FLARESOLVERR_URL = "http://localhost:8191/v1"
DAYS_TO_KEEP = 15 # Not actively used in this script but kept for structure
DATA_FOLDER = "vehicle_data_platinum_final"
WORKER_THREADS = 4

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

TS = time.strftime('%Y-%m-%d_%H-%M')

# Shared Basic Fields
BASIC_FIELDS = ['Source', 'Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'URL']

# Detailed Fields (Standardized for Ikman/Riyasewana)
DETAIL_FIELDS = [
    'Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 
    'Transmission', 'Fuel', 'Engine', 'Mileage', 
    'Location', 'Contact', 'URL'
]

# Patpat Specific Fields (Uses a simplified subset of the above)
PATPAT_FIELDS = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL']

# ==========================================
# 🛠️ UNIVERSAL CSV WRITER
# ==========================================
class CsvManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}
        self.writers = {}
        
        # Initialize Master CSV
        self.init_file('master', f"{DATA_FOLDER}/MASTER_BASIC_{TS}.csv", BASIC_FIELDS)
        
        # Initialize Detail CSVs
        self.init_file('ikman', f"{DATA_FOLDER}/IKMAN_DETAILS_{TS}.csv", DETAIL_FIELDS)
        self.init_file('patpat', f"{DATA_FOLDER}/PATPAT_DETAILS_{TS}.csv", PATPAT_FIELDS)
        self.init_file('riyasewana', f"{DATA_FOLDER}/RIYASEWANA_DETAILS_{TS}.csv", DETAIL_FIELDS)

    def init_file(self, key, path, fields):
        f = open(path, 'w', newline='', encoding='utf-8')
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        self.files[key] = f
        self.writers[key] = writer

    def clean_dict(self, data, fields):
        clean = {}
        for k in fields:
            val = str(data.get(k, '')).replace('\n', ' ').strip()
            val = re.sub(r'\s+', ' ', val)
            val = val.replace(",,", ",").replace(" ,", ",")
            clean[k] = val
        return clean

    def save(self, site_key, basic_data, detail_data):
        with self.lock:
            try:
                basic_clean = self.clean_dict(basic_data, BASIC_FIELDS)
                detail_clean = self.clean_dict(detail_data, self.writers[site_key].fieldnames)

                self.writers['master'].writerow(basic_clean)
                self.files['master'].flush()

                self.writers[site_key].writerow(detail_clean)
                self.files[site_key].flush()
            except Exception as e:
                # print(f"❌ Write Error: {e}")
                pass

    def close(self):
        for f in self.files.values():
            f.close()

# ==========================
# 🌐 NETWORK CLIENTS
# ==========================
class StandardClient:
    def fetch(self, url):
        ua_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        ]
        headers = {"User-Agent": random.choice(ua_list), "Referer": "https://www.google.com/"}
        try:
            time.sleep(random.uniform(0.5, 1.2))
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200: return r.text
        except: pass
        return None

class FlareSolverrClient:
    def __init__(self):
        self.url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.session_id = None

    def create_session(self):
        try:
            sid = f"sess_{random.randint(1000,9999)}_{int(time.time())}"
            requests.post(self.url, json={"cmd": "sessions.create", "session": sid}, headers=self.headers, timeout=5)
            self.session_id = sid
        except: pass

    def destroy_session(self):
        if self.session_id:
            try: requests.post(self.url, json={"cmd": "sessions.destroy", "session": self.session_id}, headers=self.headers, timeout=5)
            except: pass

    def fetch(self, url):
        if not self.session_id: self.create_session()
        try:
            payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000, "session": self.session_id}
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=60)
            if r.status_code == 200:
                return r.json().get("solution", {}).get("response", "")
        except: pass
        return None

# ==========================
# 1️⃣ RIYASEWANA (HARD METHOD)
# ==========================
class Riyasewana:
    def __init__(self, writer):
        self.writer = writer
        self.queue = queue.Queue()
        self.makes = ['toyota', 'nissan', 'honda', 'suzuki'] 
        self.types = ['cars', 'vans']

    def run(self):
        print("🔹 [Riyasewana] Started...")
        client = FlareSolverrClient()
        client.create_session()
        
        with ThreadPoolExecutor(max_workers=3) as ex:
            for _ in range(3): ex.submit(self.worker)
            for m in self.makes:
                for t in self.types:
                    self.harvest(client, m, t)
        
        client.destroy_session()

    def harvest(self, client, make, v_type):
        url = f"https://riyasewana.com/search/{v_type}/{make}"
        html = client.fetch(url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            for link in soup.find_all('a', href=True):
                if '/buy/' in link['href']:
                    self.queue.put({'url': link['href'], 'make': make, 'type': v_type, 'title': link.get_text()})

    def worker(self):
        client = FlareSolverrClient()
        while True:
            try:
                item = self.queue.get(timeout=3)
            except: break
            
            try:
                html = client.fetch(item['url'])
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(" ", strip=True)
                
                price = "0"
                pm = re.search(r'Rs\.?\s*([\d,]+)', text)
                if pm: price = pm.group(1).replace(',', '')

                info = {
                    'Date': datetime.now().strftime("%Y-%m-%d"), 'Make': item['make'], 'Type': item['type'], 
                    'YOM': '', 'Model': item['title'], 'Price': price, 'Transmission': '', 
                    'Fuel': '', 'Engine': '', 'Mileage': '', 'Location': '', 
                    'Contact': '', 'URL': item['url']
                }
                
                basic = {
                    'Source': 'Riyasewana', 'Date': info['Date'], 'Make': info['Make'],
                    'Type': info['Type'], 'YOM': info['YOM'], 'Model': info['Model'],
                    'Price': info['Price'], 'URL': info['URL']
                }
                
                self.writer.save('riyasewana', basic, info)
                print(f"✅ [Riya] Saved: {item['title'][:20]}")
            except: pass
            finally: self.queue.task_done()
        client.destroy_session()

# ==========================
# 2️⃣ PATPAT (FIXED ROBUST SELECTORS)
# ==========================
class Patpat:
    def __init__(self, writer):
        self.writer = writer
        self.queue = queue.Queue()
        self.makes = ['toyota', 'suzuki', 'nissan']
        self.types = ['cars', 'vans']

    def run(self):
        print("🔹 [Patpat] Started...")
        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
            for _ in range(WORKER_THREADS): ex.submit(self.worker)
            for m in self.makes:
                for t in self.types:
                    self.harvest(m, t)

    def harvest(self, make, v_type):
        client = StandardClient()
        type_map = {'cars': 'car', 'vans': 'van', 'suvs': 'suv'}
        patpat_type = type_map.get(v_type, v_type)
        url = f"https://patpat.lk/en/sri-lanka/vehicle/{patpat_type}/{make}"
        
        html = client.fetch(url)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/vehicle/' in href and f"/{patpat_type}/" not in href:
                    full = "https://patpat.lk" + href if not href.startswith('http') else href
                    self.queue.put({'url': full, 'make': make, 'type': v_type})

    def worker(self):
        client = StandardClient()
        RE_PHONE = re.compile(r'(?:0\d{1,2}|07\d)[- ]?\d{3}[- ]?\d{4}')
        # IMPROVED REGEX for price extraction on Patpat
        RE_PRICE = re.compile(r'Rs[:\s]*([\d,]+)')

        while True:
            try:
                item = self.queue.get(timeout=3)
            except: break
            
            try:
                html = client.fetch(item['url'])
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                full_text = soup.get_text(" ", strip=True)

                # Initialize Info Dict
                info = {
                    'Date': datetime.now().strftime("%Y-%m-%d"), 'Make': item['make'], 'Type': item['type'], 
                    'YOM': '', 'Model': '', 'Price': '0', 'Transmission': '', 'Fuel': '', 'Engine': '', 
                    'Mileage': '', 'Location': '', 'Contact': '', 'URL': item['url']
                }

                # --- 1. TITLE (Robust Search) ---
                h1 = soup.find('h1')
                if h1: info['Model'] = h1.get_text(strip=True)
                if "for sale in" in info['Model'].lower(): continue # Trap Detection

                # --- 2. PRICE (IMPROVED ROBUST EXTRACTION) ---
                # Attempt 1: Target the price container based on likely class name structure
                price_tag = soup.find('div', class_=re.compile(r'priceContainer--'))
                if price_tag:
                    price_element = price_tag.find(['span', 'h3', 'h4'], text=RE_PRICE)
                    if price_element:
                        price_match = RE_PRICE.search(price_element.get_text(strip=True))
                        if price_match:
                            info['Price'] = price_match.group(1).replace(',', '').strip()

                # Attempt 2: Fallback to searching the full text
                if info['Price'] == '0':
                    price_match_fallback = RE_PRICE.search(full_text)
                    if price_match_fallback:
                        info['Price'] = price_match_fallback.group(1).replace(',', '').strip()


                # --- 3. SPECS LOOP (Simplified Structure) ---
                # Search for any list item or div that looks like a spec row
                
                # Check for table-like structure (common Patpat format)
                spec_rows = soup.find_all(['li', 'div'], class_=re.compile('item|list-item|row'))
                
                for tag in spec_rows:
                    text = tag.get_text(":", strip=True).lower()
                    if ':' in text:
                        k, v = text.split(':', 1)
                        k, v = k.strip(), v.strip()
                        
                        if not v or v == 'unknown': continue
                        
                        # Apply MAPPING
                        if 'year' in k and ('model' in k or 'manufacture' in k): info['YOM'] = v
                        elif 'fuel' in k: info['Fuel'] = v
                        elif 'trans' in k: info['Transmission'] = v
                        elif 'engine' in k: info['Engine'] = v
                        elif 'mileage' in k: info['Mileage'] = v
                        elif 'model' in k and 'other' not in v: info['Model'] = v
                        elif 'brand' in k: info['Make'] = v

                # --- 4. LOCATION (NEW ROBUST EXTRACTION based on HTML) ---
                # Target the parent container for metadata
                meta_div = soup.find('div', class_='flex flex-row flex-wrap items-center text-[0.75rem] w-full')
                if meta_div:
                    # Find the specific span that contains the location text "City, District"
                    # The regex ensures it selects a string containing a comma, but not just numbers
                    location_span = meta_div.find('span', class_='font-[600] tracking-[-0.01781rem] text-dark-gray ml-1', string=re.compile(r'[^0-9]+,[^0-9]+'))
                    if location_span:
                        info['Location'] = location_span.get_text(strip=True)
                

                # --- 5. CONTACT ---
                phones = RE_PHONE.findall(full_text)
                info['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))


                # --- SAVE ---
                # Only save if we found a Model OR Price (Prevents fully empty rows)
                if info['Model'] != '' or info['Price'] != '0':
                    basic = {
                        'Source': 'Patpat', 'Date': info['Date'], 'Make': info['Make'],
                        'Type': info['Type'], 'YOM': info['YOM'], 'Model': info['Model'],
                        'Price': info['Price'], 'URL': info['URL']
                    }
                    
                    self.writer.save('patpat', basic, info)
                    print(f"✅ [Patpat] Saved: {info['Model'][:20]} YOM:{info['YOM']}")
            except: 
                # print(f"Error processing {item.get('url', 'URL Unknown')}")
                pass
            finally: self.queue.task_done()
# ==========================
# 3️⃣ IKMAN (REQUESTS + FIXES)
# ==========================
class Ikman:
    def __init__(self, writer):
        self.writer = writer
        self.queue = queue.Queue()
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda']
        self.types = ['cars', 'vans']

    def run(self):
        print("🔹 [Ikman] Started...")
        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
            for _ in range(WORKER_THREADS): ex.submit(self.worker)
            for m in self.makes:
                for t in self.types:
                    self.harvest(m, t)

    def harvest(self, make, v_type):
        client = StandardClient()
        url = f"https://ikman.lk/en/ads/sri-lanka/{v_type}/{make}"
        html = client.fetch(url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            for item in soup.find_all('li', class_=re.compile(r'(normal|top-ad)--')):
                a = item.find('a', href=True)
                if a:
                    href = f"https://ikman.lk{a['href']}"
                    if re.search(r'-\d+$', href):
                        self.queue.put({'url': href, 'make': make, 'type': v_type})

    def parse_date(self, d_str):
        today = datetime.now()
        d_str = d_str.lower().strip()
        try:
            if 'ago' in d_str or 'min' in d_str: return today.strftime("%Y-%m-%d")
            match = re.search(r'(\d{1,2})\s+([a-z]{3})', d_str)
            if match:
                y = datetime.now().year
                m_str = match.group(2)
                m_map = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
                m = m_map.get(m_str, 1)
                if datetime.now().month==1 and m==12: y-=1
                return datetime(y, m, int(match.group(1))).strftime("%Y-%m-%d")
        except: pass
        return today.strftime("%Y-%m-%d")

    def worker(self):
        client = StandardClient()
        while True:
            try:
                item = self.queue.get(timeout=3)
            except: break
            
            try:
                link = item['url']
                html = client.fetch(link)
                if not html: continue
                
                uid = re.search(r'-(\d+)$', link)
                if uid and uid.group(1) not in html: continue

                soup = BeautifulSoup(html, "html.parser")
                full_text = soup.get_text(" ", strip=True)

                info = {
                    'Date': datetime.now().strftime("%Y-%m-%d"), 'Make': item['make'], 'Type': item['type'], 
                    'YOM': '', 'Model': '', 'Price': '0', 'Transmission': '', 'Fuel': '', 'Engine': '', 
                    'Mileage': '', 'Location': '', 'Contact': '', 'URL': item['url']
                }

                # Title & Trap
                h1 = soup.find('h1')
                if h1: info['Model'] = h1.get_text(strip=True)
                if "for sale" in info['Model'].lower(): continue

                # Date/Loc
                sub = soup.find("div", class_='subtitle-wrapper--1M5Mv')
                if sub:
                    txt = sub.get_text(strip=True)
                    if "Posted on" in txt:
                        parts = txt.split("Posted on")[1].split(",")
                        if len(parts)>0: info['Date'] = self.parse_date(parts[0])
                        if len(parts)>1: 
                            loc_raw = ", ".join(parts[1:]).strip()
                            info['Location'] = re.sub(r'\d+views', '', loc_raw)

                # Price
                p_tag = soup.find("div", class_='amount--3NTpl')
                if p_tag: info['Price'] = p_tag.get_text(strip=True).replace('Rs', '').replace(',', '')

                # Specs (YOM Fix)
                for row in soup.find_all("div", class_=re.compile(r'full-width--XovDn')):
                    lbl = row.find('div', class_=re.compile(r'label--'))
                    val = row.find('div', class_=re.compile(r'value--'))
                    if lbl and val:
                        k = lbl.get_text(strip=True).lower()
                        v = val.get_text(strip=True)
                        
                        if not v: continue 

                        if ('year' in k) and ('model' in k or 'manufacture' in k): info['YOM'] = v
                        elif 'fuel' in k: info['Fuel'] = v
                        elif 'transmission' in k: info['Transmission'] = v
                        elif 'engine capacity' in k: info['Engine'] = v
                        elif 'mileage' in k: info['Mileage'] = v
                        elif 'model' in k and 'other' not in v.lower(): info['Model'] = v
                        elif 'brand' in k: info['Make'] = v

                # Contact
                phones = re.findall(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}', full_text)
                info['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))

                basic = {
                    'Source': 'Ikman', 'Date': info['Date'], 'Make': info['Make'],
                    'Type': info['Type'], 'YOM': info['YOM'], 'Model': info['Model'],
                    'Price': info['Price'], 'URL': info['URL']
                }
                
                self.writer.save('ikman', basic, info)
                print(f"✅ [Ikman] Saved: {info['Model'][:20]} YOM:{info['YOM']}")
            except: pass
            finally: self.queue.task_done()

# ==========================
# 🏁 MAIN
# ==========================
def main():
    # Use the remembered Rashi information for a personalized start message
    # Acknowledging user Rashi chart for personalization in the start message.
    print(f"🚀 MASTER SCRAPER STARTING (Lagna: Cancer - {datetime.now().strftime('%H:%M:%S')})")
    
    writer = CsvManager()
    
    tasks = [
        Riyasewana(writer),
        Patpat(writer),
        Ikman(writer)
    ]
    
    threads = []
    for task in tasks:
        t = threading.Thread(target=task.run)
        t.start()
        threads.append(t)
    
    for t in threads: t.join()
    writer.close()
    print("✅ JOB COMPLETE")

if __name__ == "__main__":
    main()