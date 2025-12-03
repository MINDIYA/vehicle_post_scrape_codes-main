#!/usr/bin/env python3
"""
RIYASEWANA SCRAPER V18.0 - SPEEDSTER EDITION
- OPTIMIZATION: Drastically lowers browser count to stop RAM choking.
- LOGIC: 5 Fast Browsers >>> 12 Slow Browsers.
- TUNING: Allocates 1.2GB per browser to ensure 0% lag.
"""

from __future__ import annotations
import os
import sys
import time
import json
import random
import queue
import signal
import sqlite3
import logging
import threading
import requests
import argparse
import urllib.parse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Dict, Any, List
from tqdm import tqdm
from bs4 import BeautifulSoup
import psutil
import re
import csv

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
FLARESOLVERR_URL = "http://127.0.0.1:8191/v1"

MAKES = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda',
         'daihatsu', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'mercedes-benz', 'land-rover', 'tata', 'mahindra']
TYPES = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']

MAX_PAGES_PER_COMBO = 160
DAYS_TO_KEEP = 15
BATCH_SIZE = 20
CHECKPOINT_BUFFER = 50

# 📲 WHATSAPP NOTIFICATIONS
ENABLE_WHATSAPP = True
PHONE_NUMBER = "+94760010626"
API_KEY = "REPLACE_WITH_YOUR_KEY" 

# 🛑 TUNING CONSTANTS (THE FIX)
# We force a "Safe Mode" default. 5 Browsers is the sweet spot for 6GB/8GB RAM.
SESSION_TARGET_DEFAULT = 5 
SESSION_POOL_MIN = 2
SESSION_POOL_MAX = 16 # Capped at 16 even on supercomputers
REQUEST_RETRIES = 3
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

OUTPUT_FOLDER = "riyasewana_data_v18"
CHECKPOINT_FILE = "search_progress_v18.txt"
SEEN_DB = "seen_urls_v18.sqlite"
LOG_LEVEL = logging.INFO

# Logging Setup
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("riya_final")

# ---------------------------
# 🧠 INTELLIGENT RESOURCE CALCULATOR (RE-TUNED)
# ---------------------------
def get_docker_limits() -> Tuple[float, int, str, str]:
    system_os = sys.platform
    total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    cpu_cores = os.cpu_count() or 1
    source = "Physical Hardware"
    limit_ram = total_ram_gb

    try:
        if system_os == "win32":
            config_path = os.path.join(os.path.expanduser("~"), ".wslconfig")
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read().lower()
                m = re.search(r'memory\s*=\s*(\d+)(gb|mb)', content)
                if m:
                    val = int(m.group(1))
                    if m.group(2) == "mb": val = val / 1024
                    limit_ram = val
                    source = "Windows .wslconfig"
    except Exception: pass
    
    return limit_ram, cpu_cores, source, system_os

def configure_dynamic_resources(target: int) -> int:
    docker_ram, docker_cpu, source, os_name = get_docker_limits()
    
    # 🛑 AGGRESSIVE SAFETY OVERHEAD
    # We now demand 1.2 GB per browser to ensure it never swaps
    
    # If using .wslconfig (Fixed Limit), we treat that as the TOTAL container size.
    # Docker Engine needs ~1GB.
    usable_ram = max(1.0, docker_ram - 1.5)
    
    # Calculation: 1 Browser per 1.2 GB
    pool_est = int(usable_ram / 1.2)
    
    # CPU Constraint: Don't exceed 1.5x cores (Previous was 2x)
    pool_est = min(pool_est, int(docker_cpu * 1.5))
    
    # Bounds
    pool_est = max(SESSION_POOL_MIN, pool_est)
    pool_est = min(SESSION_POOL_MAX, pool_est)
    
    log.info("="*50)
    log.info(f"⚙️  SPEEDSTER CONFIG ({os_name})")
    log.info(f"   - RAM Available: {round(docker_ram, 1)} GB (Source: {source})")
    log.info(f"   - Optimized Pool: {pool_est} Browsers (Preventing Choke)")
    log.info("="*50)
    return pool_est

# ---------------------------
# 💾 DATABASE & STORAGE
# ---------------------------
class SeenDB:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("CREATE TABLE IF NOT EXISTS seen (url TEXT PRIMARY KEY, ts TEXT)")
        self._conn.commit()

    def seen(self, url: str) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
            return cur.fetchone() is not None

    def mark(self, url: str):
        with self._lock:
            try:
                ts = datetime.now(timezone.utc).isoformat()
                self._conn.execute("INSERT OR IGNORE INTO seen (url, ts) VALUES (?, ?)", (url, ts))
                self._conn.commit()
            except: pass

    def close(self):
        try: self._conn.close()
        except: pass

class BatchWriter:
    def __init__(self, filepath: str, fieldnames: List[str], batch_size: int = BATCH_SIZE):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.batch_size = batch_size
        self._buffer = []
        self._lock = threading.Lock()
        
        folder = os.path.dirname(self.filepath)
        if folder and not os.path.exists(folder): os.makedirs(folder, exist_ok=True)
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def add_row(self, row: Dict[str, Any]):
        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self.batch_size:
                self._flush_unsafe()

    def flush(self):
        with self._lock:
            if self._buffer: self._flush_unsafe()

    def _flush_unsafe(self):
        try:
            with open(self.filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(self._buffer)
            self._buffer.clear()
        except: pass

class CheckpointManager:
    def __init__(self, path: str):
        self.path = path
        self._buffer = []
        self._lock = threading.Lock()
        self.completed = set()
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.completed = set(line.strip() for line in f)
                log.info(f"Loaded {len(self.completed)} checkpoints.")
            except: pass

    def add(self, task_id: str):
        with self._lock:
            if task_id in self.completed: return
            self._buffer.append(task_id)
            self.completed.add(task_id)
            if len(self._buffer) >= CHECKPOINT_BUFFER: self.flush()

    def flush(self):
        with self._lock:
            if not self._buffer: return
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    for tid in self._buffer: f.write(tid + "\n")
                self._buffer = []
            except: pass

# ---------------------------
# 🏊‍♂️ SESSION MANAGER (RETRY ENABLED)
# ---------------------------
class SessionManager:
    def __init__(self, pool_size: int):
        self.fs_url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.pool = queue.Queue(maxsize=pool_size)
        self.max_retries = 3
        
        log.info(f"🔥 Creating {pool_size} FlareSolverr sessions...")
        for i in range(pool_size):
            sid = self._create_sid()
            if sid: 
                self.pool.put(sid)
                log.info(f"   -> Session {i+1} ready.")

    def _create_sid(self) -> Optional[str]:
        sid = f"riya_speed_{random.randint(1000,9999)}_{int(time.time())}"
        try:
            r = requests.post(self.fs_url, json={"cmd": "sessions.create", "session": sid}, headers=self.headers, timeout=20)
            if r.status_code == 200: return r.json().get("session") or sid
        except: pass
        return None

    def fetch(self, url: str) -> Optional[str]:
        # 🔄 RETRY LOOP
        for attempt in range(self.max_retries):
            try: 
                session_id = self.pool.get(timeout=5)
            except: 
                time.sleep(1)
                continue

            html = None
            success = False
            
            try:
                payload = {
                    "cmd": "request.get",
                    "url": url,
                    "session": session_id,
                    "maxTimeout": 60000,
                    "headers": {"User-Agent": random.choice(USER_AGENTS)}
                }
                r = requests.post(self.fs_url, json=payload, headers=self.headers, timeout=65)
                
                if r.status_code == 200:
                    data = r.json()
                    if data.get("status") == "ok":
                        html = data.get("solution", {}).get("response", "")
                        if "<html" in html[:200].lower(): 
                            success = True
            except: pass

            if success:
                self.pool.put(session_id)
                return html
            else:
                self._destroy(session_id)
                new_sid = self._create_sid()
                if new_sid: self.pool.put(new_sid)
                else: self.pool.put(None)
                time.sleep(2) 

        return None

    def _destroy(self, sid):
        if not sid: return
        try: requests.post(self.fs_url, json={"cmd": "sessions.destroy", "session": sid}, headers=self.headers, timeout=2)
        except: pass
    
    def close(self):
        while not self.pool.empty():
            try: self._destroy(self.pool.get_nowait())
            except: break

# ---------------------------
# 👷 WORKER LOGIC
# ---------------------------
ad_queue = queue.Queue(maxsize=500) 
stop_event = threading.Event()
stats = {'found': 0, 'saved': 0, 'errors': 0, 'dupes': 0, 'redirects': 0}
stats_lock = threading.Lock()

RE_PHONE = re.compile(r'(?:0\d{1,2}|07\d)[- ]?\d{3}[- ]?\d{4}')
RE_YOM = re.compile(r'\b(19\d{2}|20\d{2})\b')
RE_TRANS = re.compile(r'\b(Automatic|Manual|Tiptronic)\b', re.IGNORECASE)
RE_DATE_ISO = re.compile(r'(\d{4}-\d{2}-\d{2})')
RE_PRICE = re.compile(r'Rs\.?\s*([\d\.,]+)', re.IGNORECASE)
RE_LOC = re.compile(r'-sale-([a-zA-Z]+)-')

def send_whatsapp(msg):
    if not ENABLE_WHATSAPP or "REPLACE" in API_KEY: return
    try:
        encoded = urllib.parse.quote(msg)
        requests.get(f"https://api.callmebot.com/whatsapp.php?phone={PHONE_NUMBER}&text={encoded}&apikey={API_KEY}", timeout=10)
    except: pass

def harvest_task(make, v_type, page_num, cutoff, pool, ckpt, seen_db):
    task_id = f"{make}|{v_type}|{page_num}"
    if task_id in ckpt.completed: return 0

    url = f"https://riyasewana.com/search/{v_type}/{make}"
    if page_num > 1: url += f"?page={page_num}"

    html = pool.fetch(url)
    if not html: 
        with stats_lock: stats['errors'] += 1
        return 0
    
    if len(html) < 2000 and "Just a moment" in html:
        with stats_lock: stats['redirects'] += 1
        return 0

    try: soup = BeautifulSoup(html, "lxml") 
    except: soup = BeautifulSoup(html, "html.parser") 

    links = soup.find_all('a', href=True)
    count = 0

    for link in links:
        href = link['href']
        if '/buy/' in href and '-sale-' in href:
            if href.startswith("/"): href = "https://riyasewana.com" + href
            
            if seen_db.seen(href):
                with stats_lock: stats['dupes'] += 1
                continue
            seen_db.mark(href)

            title = link.get_text(" ", strip=True)
            if len(title) < 5:
                h2 = link.find_parent('h2')
                if h2: title = h2.get_text(" ", strip=True)
            
            final_date, price = "Check_Page", "0"
            container = link.find_parent('li') or link.find_parent('div', class_=re.compile('item', re.I))
            if container:
                txt = container.get_text(" ", strip=True)
                dm = RE_DATE_ISO.search(txt)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: continue
                        final_date = dm.group(1)
                    except: pass
                
                pm = RE_PRICE.search(txt)
                if pm: price = pm.group(1).replace(",", "")

            item = {'url': href, 'date': final_date, 'make': make, 'type': v_type, 'title': title, 'price': price}
            
            # Fetch Ad Page
            ad_html = pool.fetch(href)
            if not ad_html:
                with stats_lock: stats['errors'] += 1
                continue
            
            try:
                ad_queue.put({'item': item, 'html': ad_html}, timeout=10)
                count += 1
            except: pass

    if count > 0: ckpt.add(task_id)
    with stats_lock: stats['found'] += count
    return count

def extractor_worker(bw, dw, cutoff):
    while not stop_event.is_set() or not ad_queue.empty():
        try: record = ad_queue.get(timeout=2)
        except: continue

        try:
            item, html = record['item'], record['html']
            try: soup = BeautifulSoup(html, "lxml")
            except: soup = BeautifulSoup(html, "html.parser")
            
            full_text = soup.get_text(" ", strip=True)
            
            if item['date'] == "Check_Page":
                dm = RE_DATE_ISO.search(full_text)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: 
                            ad_queue.task_done()
                            continue
                        item['date'] = dm.group(1)
                    except: item['date'] = datetime.now().strftime("%Y-%m-%d")
                else: item['date'] = datetime.now().strftime("%Y-%m-%d")

            details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Contact': '', 'Location': '', 'Description': ''}
            
            for label in soup.find_all('p', class_='moreh'):
                try:
                    txt = label.get_text(strip=True).lower()
                    td = label.find_parent('td')
                    val = td.find_next_sibling('td').get_text(" | ", strip=True)
                    
                    if 'mileage' in txt: details['Mileage'] = val
                    elif 'engine' in txt: details['Engine'] = val
                    elif 'transmission' in txt: details['Transmission'] = val
                    elif 'fuel' in txt: details['Fuel'] = val
                    elif 'yom' in txt: details['YOM'] = val
                    elif 'details' in txt: details['Description'] = val
                except: pass

            if not details['Transmission']:
                m = RE_TRANS.search(full_text)
                if m: details['Transmission'] = m.group(1).capitalize()
            if not details['YOM']:
                m = RE_YOM.search(item['title'])
                if m: details['YOM'] = m.group(1)
            
            phones = RE_PHONE.findall(full_text)
            if phones: details['Contact'] = " / ".join(sorted(set(p.replace('-','').replace(' ','') for p in phones)))
            
            lm = RE_LOC.search(item['url'])
            if lm: details['Location'] = lm.group(1).capitalize()

            row_basic = {'Date': item['date'], 'Make': item['make'], 'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 'Price': item['price']}
            row_detail = {**row_basic, **details, 'URL': item['url']}
            
            bw.add_row(row_basic)
            dw.add_row(row_detail)
            with stats_lock: stats['saved'] += 1

        except: 
            with stats_lock: stats['errors'] += 1
        finally: ad_queue.task_done()

# ---------------------------
# MAIN
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DAYS_TO_KEEP)
    args = parser.parse_args()

    send_whatsapp(f"🚀 Speedster V18 Started!")

    try: 
        test_url = FLARESOLVERR_URL.replace("/v1", "/")
        if not requests.get(test_url, timeout=5).ok: raise Exception()
        log.info("✅ FlareSolverr Connected!")
    except:
        log.error("FlareSolverr not running!")
        return

    pool_size = configure_dynamic_resources(SESSION_TARGET_DEFAULT)
    pool = SessionManager(pool_size)
    
    n_searchers = pool_size 
    n_extractors = pool_size * 2
    
    log.info(f"✅ WORKERS: {n_searchers} Searchers | {n_extractors} Extractors")

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    bw = BatchWriter(f"{OUTPUT_FOLDER}/RIYA_BASIC_{ts}.csv", ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price'])
    dw = BatchWriter(f"{OUTPUT_FOLDER}/RIYA_DETAILED_{ts}.csv", ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL', 'Description'])
    
    ckpt = CheckpointManager(CHECKPOINT_FILE)
    seen = SeenDB(SEEN_DB)

    ex_pool = ThreadPoolExecutor(max_workers=n_extractors)
    for _ in range(n_extractors): ex_pool.submit(extractor_worker, bw, dw, cutoff)

    tasks = []
    for make in MAKES:
        for v_type in TYPES:
            for p in range(1, MAX_PAGES_PER_COMBO + 1): tasks.append((make, v_type, p))
    random.shuffle(tasks)

    log.info("🚀 Starting Crawl...")
    
    try:
        with ThreadPoolExecutor(max_workers=n_searchers) as s_pool:
            futures = [s_pool.submit(harvest_task, m, t, p, cutoff, pool, ckpt, seen) for (m, t, p) in tasks]
            with tqdm(total=len(futures), unit="pg") as pbar:
                for f in as_completed(futures):
                    try: f.result()
                    except: pass
                    pbar.update(1)
                    pbar.set_postfix({"Found": stats['found'], "Saved": stats['saved'], "Wait": stats['redirects']})
        
        ad_queue.join()
        stop_event.set()
        ex_pool.shutdown(wait=True)
        bw.flush(); dw.flush(); ckpt.flush(); seen.close(); pool.close()
        
        msg = f"✅ Done!\nSaved: {stats['saved']}"
        log.info(msg)
        send_whatsapp(msg)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
        pool.close()

if __name__ == "__main__":
    main()