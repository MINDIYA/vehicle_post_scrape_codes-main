#!/usr/bin/env python3
"""
29_scrape_patpat_dom_fix.py
---------------------------------------------------------
DOM-SPECIFIC EXTRACTOR (Fixes "Unknown" & Footer Junk)
1. Targets Tailwind "justify-between" rows for specs (Mileage, Model, etc.).
2. Extracts Seller Name relative to the H1 Title.
3. Isolates Description to the "Description Overview" block only.
4. Adds robust checks for missing fields and uses fallbacks.
5. IMPROVED: Handles variable HTML structures for specs.
6. REFINED: stricter description parsing to avoid footer menu.
7. FIXED: Engine Capacity extraction.
8. FIXED: Description cleanup (Targets specific toggle box).
9. FIXED: Contact Number extraction (Grabs hidden tel: link).
---------------------------------------------------------
"""

import time
import requests
import random
import csv
import re
import threading
import queue
import os
import traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
cpu_count = os.cpu_count() or 4
max_threads = cpu_count * 4
SAFE_CAP = 35 
if max_threads > SAFE_CAP: max_threads = SAFE_CAP

SEARCH_WORKERS = max(2, int(max_threads * 0.2))
DETAIL_WORKERS = max_threads - SEARCH_WORKERS

print("="*60)
print(f"🚀 DOM FIX MODE: {SEARCH_WORKERS} Harvesters | {DETAIL_WORKERS} Extractors")
print("="*60)

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
total_saved_count = 0
saved_count_lock = threading.Lock()

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
    if not html: return 0

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
            final_date = "Check_Page" 
            
            if date_match:
                d_str = date_match.group(1)
                try:
                    if datetime.strptime(d_str, "%Y-%m-%d") < cutoff_date: 
                        continue 
                    final_date = d_str
                except: pass
            
            search_price = "0"
            price_match = re.search(r'Rs[:\.]?\s*([\d,]+)', card_text)
            if price_match:
                search_price = price_match.group(1).replace(',', '')

            raw_title = link.text.strip() or "Unknown"

            item = {
                'url': href,
                'date': final_date,
                'make': brand,
                'title': raw_title,
                'search_price': search_price
            }
            ad_queue.put(item)
            count += 1
            
    return count

# ---------------------------------------------------------
# WORKER 2: THE EXTRACTOR (DOM-SPECIFIC FIX)
# ---------------------------------------------------------
def extractor_worker(client, basic_writer, detail_writer, lock, f_basic, f_detail, cutoff_date):
    global total_saved_count
    
    while not stop_event.is_set() or not ad_queue.empty():
        try:
            item = ad_queue.get(timeout=1) 
        except queue.Empty:
            continue

        try:
            url = item['url']
            html = client.fetch(url)
            
            if not html:
                ad_queue.task_done()
                continue

            soup = BeautifulSoup(html, "html.parser")
            
            # --- 0. DATE CHECK ---
            final_date = item['date']
            if final_date == "Check_Page":
                # Try to find date on detail page (look for YYYY-MM-DD pattern)
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', soup.get_text())
                if date_match:
                    found_date = date_match.group(1)
                    try:
                         if datetime.strptime(found_date, "%Y-%m-%d") < cutoff_date:
                             ad_queue.task_done()
                             continue
                         final_date = found_date
                    except: pass
                else:
                    final_date = datetime.now().strftime("%Y-%m-%d")

            details = {
                'Price': item.get('search_price', '0'),
                'Model': "Unknown",
                'Year': "",
                'Transmission': "Unknown",
                'Contact': "Unknown",
                'Description': "",
                'Engine': ""
            }

            # --- 1. GRID PARSER (Updated Logic) ---
            
            specs_map = {
                'Year': ['Model Year', 'YOM', 'Year', 'Register Year'],
                'Model': ['Model'],
                'Transmission': ['Transmission', 'Gear'], 
                'Fuel': ['Fuel Type', 'Fuel'],
                'Engine': ['Engine Capacity', 'Engine', 'Capacity'],
                'Mileage': ['Mileage']
            }

            def clean_val(v): return v.replace(":", "").strip()

            # Find potential containers (Core Info or Key Details)
            containers = soup.find_all('div', class_=lambda x: x and ('grid' in x or 'flex-col' in x))
            
            found_specs = False
            for container in containers:
                # Check if this container holds our specs
                container_text = container.get_text().lower()
                if not any(lbl.lower() in container_text for lbl in ['model', 'transmission', 'fuel', 'mileage']):
                    continue

                # Iterate children to find pairs
                # Handles <div><div>Label</div><div>Value</div></div> structure
                for child in container.find_all('div', recursive=False):
                     child_text = child.get_text(" ", strip=True)
                     for key, labels in specs_map.items():
                        if key in details and details[key] != "Unknown" and details[key] != "": continue
                        
                        for label in labels:
                             if label.lower() in child_text.lower():
                                  # Try to split the text
                                  parts = re.split(re.escape(label), child_text, flags=re.IGNORECASE)
                                  if len(parts) > 1:
                                       val = clean_val(parts[1])
                                       if val:
                                            details[key] = val
                                            found_specs = True
                
                if found_specs: break

            # Fallback: Search specifically for label elements if container method failed
            if not found_specs:
                for key, labels in specs_map.items():
                    for label in labels:
                        if key in details and details[key] != "Unknown" and details[key] != "": break
                        
                        label_el = soup.find(string=lambda text: text and label.lower() == text.strip().lower())
                        if label_el:
                            try:
                                # Strategy 1: Next text node
                                next_text = label_el.find_next(string=True)
                                if next_text and next_text.strip() and next_text.strip().lower() != label.lower():
                                    details[key] = clean_val(next_text)
                                    continue
                                
                                # Strategy 2: Parent's next sibling
                                parent = label_el.find_parent()
                                if parent:
                                    next_sibling = parent.find_next_sibling()
                                    if next_sibling:
                                        val = next_sibling.get_text(strip=True)
                                        if val:
                                            details[key] = clean_val(val)
                            except: pass

            # --- 2. TITLE & DESC PARSING (Fallback) ---
            full_text = soup.get_text(" ", strip=True)
            
            if details['Model'] == "Unknown":
                h1 = soup.find('h1')
                page_title = h1.get_text(strip=True) if h1 else item['title']
                
                make = item['make']
                clean_title = page_title.lower().replace(make.lower(), "").strip()
                clean_title = re.sub(r'\b(19|20)\d{2}\b', '', clean_title)
                clean_title = re.sub(r'[^a-zA-Z0-9\s]', '', clean_title).strip()
                
                words = clean_title.split()
                if words:
                    details['Model'] = words[0].title()
                    if len(words) > 1 and len(words[1]) > 1:
                         details['Model'] += " " + words[1].title()

            if details['Year'] == "":
                 h1 = soup.find('h1')
                 page_title = h1.get_text(strip=True) if h1 else item['title']
                 yr_match = re.search(r'\b(19\d{2}|20\d{2})\b', page_title)
                 if yr_match: details['Year'] = yr_match.group(1)


            # --- 3. TEXT SCANNING FALLBACK (Mobile friendly) ---
            if details['Transmission'] == "Unknown":
                if re.search(r'\b(Automatic|Auto)\b', full_text, re.IGNORECASE): details['Transmission'] = "Automatic"
                elif re.search(r'\b(Manual)\b', full_text, re.IGNORECASE): details['Transmission'] = "Manual"
                elif re.search(r'\b(Tiptronic)\b', full_text, re.IGNORECASE): details['Transmission'] = "Tiptronic"

            if 'Fuel' == "Unknown" or details.get('Fuel') is None: # Ensure key exists
                 details['Fuel'] = "Unknown"
            
            if details['Fuel'] == "Unknown":
                if re.search(r'\b(Hybrid)\b', full_text, re.IGNORECASE): details['Fuel'] = "Hybrid"
                elif re.search(r'\b(Electric)\b', full_text, re.IGNORECASE): details['Fuel'] = "Electric"
                elif re.search(r'\b(Diesel)\b', full_text, re.IGNORECASE): details['Fuel'] = "Diesel"
                elif re.search(r'\b(Petrol)\b', full_text, re.IGNORECASE): details['Fuel'] = "Petrol"

            # --- 4. CONTACT & DESC CLEANUP (FIXED) ---
            try:
                ptag = soup.find("div", class_=lambda x: x and ('price' in x or 'amount' in x))
                if not ptag: ptag = soup.find("h3", class_="text-green")
                
                if ptag: details['Price'] = re.sub(r'[^\d]', '', ptag.get_text(strip=True))

                # Contact - Improved to find tel link or seller name
                # 1. Look for hidden tel: link first (most accurate)
                tel_link = soup.find('a', href=re.compile(r'^tel:'))
                if tel_link:
                    details['Contact'] = tel_link['href'].replace('tel:', '')
                
                if details['Contact'] == "Unknown":
                     # Heuristic for seller name under title (e.g. "Shamika")
                     h1 = soup.find('h1')
                     if h1:
                         curr = h1.next_sibling
                         for _ in range(3):
                             if curr and curr.name in ['div', 'p', 'span']:
                                 txt = curr.get_text(strip=True)
                                 # Check if text is likely a name (no numbers, reasonable length)
                                 if txt and not re.match(r'^[\d\W]', txt) and len(txt) < 30 and "Rs" not in txt:
                                     # Split by date (YYYY-MM-DD) if present
                                     parts = re.split(r'\d{4}-\d{2}-\d{2}', txt)
                                     details['Contact'] = parts[0].strip()
                                     break
                             curr = curr.next_sibling if curr else None

                # Description - Improved to use specific toggle box
                dtag = soup.find("div", attrs={"x-show": re.compile(r"active\s*===\s*1")})
                
                if not dtag:
                    # Fallback: Look for the "Description Overview" label and get next sibling
                    desc_label = soup.find(string=re.compile("Description Overview"))
                    if desc_label:
                        parent_btn = desc_label.find_parent('button')
                        if parent_btn:
                            dtag = parent_btn.find_next_sibling('div')

                if dtag: 
                    raw_desc = dtag.get_text(" ", strip=True)
                    # Fallback cleanup just in case
                    cutoff_markers = ["Top Dealers", "Contact Us", "Blog", "English", "About patpat", "Security Guidelines", "Similar ads", "Terms of Use", "Privacy Policy"]
                    for marker in cutoff_markers:
                        if marker in raw_desc:
                            raw_desc = raw_desc.split(marker)[0]
                    details['Description'] = raw_desc[:500]
            except: pass

            # Final Clean
            if details.get('Price') == '0' or not details.get('Price'):
                details['Price'] = item.get('search_price', '')

            basic_row = {
                'Date': final_date, 'Make': item['make'],
                'Year': details.get('Year', ''), 'Model': details.get('Model', ''),
                'Transmission': details.get('Transmission', ''), 'Price': details.get('Price', '')
            }

            detail_row = {
                'Date': final_date, 'Make': item['make'],
                'Year': details.get('Year', ''), 'Model': details.get('Model', ''),
                'Transmission': details.get('Transmission', ''), 'Price': details.get('Price', ''),
                'Fuel': details.get('Fuel', ''), 'Engine': details.get('Engine', ''),
                'Mileage': details.get('Mileage', ''), 'Contact': details.get('Contact', ''),
                'Description': details.get('Description', ''), 'URL': item['url']
            }

            with lock:
                basic_writer.writerow(basic_row)
                detail_writer.writerow(detail_row)
                f_basic.flush()
                f_detail.flush()
                tqdm.write(f"   ✓ {item['make'].upper()} {details.get('Model', '?')} ({details.get('Year','?')}) [{details.get('Transmission','?')}]")
            
            with saved_count_lock:
                total_saved_count += 1

        except Exception as e:
            tqdm.write(f"⚠️ ERROR processing {item['url']}: {e}")
            
        finally:
            ad_queue.task_done()

def main():
    cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    folder = "patpat_final_data"
    if not os.path.exists(folder): os.makedirs(folder)
    
    timestamp = time.strftime('%Y-%m-%d_%H-%M')
    
    basic_csv = f"{folder}/patpat_BASIC_{timestamp}.csv"
    basic_fields = ['Date', 'Make', 'Year', 'Model', 'Transmission', 'Price']

    detail_csv = f"{folder}/patpat_DETAILED_{timestamp}.csv"
    detail_fields = ['Date', 'Make', 'Year', 'Model', 'Transmission', 'Price', 'Fuel', 'Engine', 'Mileage', 'Contact', 'Description', 'URL']

    client_harvest = FlareSolverrClient()
    client_harvest.create_session()
    client_extract = FlareSolverrClient() 
    client_extract.create_session()

    file_lock = threading.Lock()

    with open(basic_csv, 'w', newline='', encoding='utf-8') as f_basic, \
         open(detail_csv, 'w', newline='', encoding='utf-8') as f_detail:
        
        writer_basic = csv.DictWriter(f_basic, fieldnames=basic_fields)
        writer_detail = csv.DictWriter(f_detail, fieldnames=detail_fields)
        
        writer_basic.writeheader()
        writer_detail.writeheader()

        print("🚀 Starting DOM-FIX Extraction...")
        
        extraction_executor = ThreadPoolExecutor(max_workers=DETAIL_WORKERS)
        for _ in range(DETAIL_WORKERS):
            extraction_executor.submit(extractor_worker, client_extract, writer_basic, writer_detail, file_lock, f_basic, f_detail, cutoff)

        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as search_pool:
            for brand in BRANDS_TO_SCRAPE:
                futures = []
                for page in range(1, PAGES_PER_BRAND + 1):
                    futures.append(search_pool.submit(harvest_task, client_harvest, brand, page, cutoff))
                
                desc = f"Scanning {brand.upper()}"
                with tqdm(total=len(futures), desc=desc, unit="pg") as pbar:
                    for future in as_completed(futures):
                        try:
                            count = future.result()
                            pbar.set_postfix(found=count, saved=total_saved_count)
                            pbar.update(1)
                        except: pbar.update(1)

        print("\n🛑 Search Complete. Finishing queue...")
        ad_queue.join()
        stop_event.set()
        extraction_executor.shutdown(wait=True)

    client_harvest.destroy_session()
    client_extract.destroy_session()
    print("\n" + "="*60)
    print(f"✅ COMPLETE")
    print(f"📂 Basic: {basic_csv}")
    print(f"📂 Detailed: {detail_csv}")
    print("="*60)

if __name__ == "__main__":
    main()