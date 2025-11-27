#!/usr/bin/env python3
"""
scrape_post.py (updated)
- Uses FlareSolverr (HTTP API) to fetch post pages (no Selenium).
- Reads a CSV with URLs (from your flaresolverr href extractor).
- Parses the page and extracts the 'moret' table values, writing them to CSV.
- Includes session management, retries, randomized delays, and cleanup.

References:
- Original FlareSolverr helper logic and main scraper: scrape_flaresolverr.py
- Original post-extraction and CSV code: scrape_post.py
"""

import time
import requests
import random
import csv
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

# -------------------------
# FlareSolverr helper logic (self-contained, no external import)
# -------------------------
class FlareSolverrClient:
    def __init__(self, flaresolverr_url="http://localhost:8191/v1", timeout=70):
        self.base_url = flaresolverr_url.rstrip('/')
        self.api_url = self.base_url  # v1 endpoint
        self.headers = {"Content-Type": "application/json"}
        self.session = None
        self.timeout = timeout

        # if not self._health_check():
        #     raise RuntimeError(
        #         "FlareSolverr health check failed. Make sure FlareSolverr is running on port 8191.\n"
        #         "Example: docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest"
        #     )

    def _health_check(self):
        try:
            resp = requests.get(f"{self.base_url}/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def create_session(self):
        payload = {"cmd": "sessions.create", "session": f"session_{random.randint(1000, 9999)}"}
        try:
            r = requests.post(self.api_url, json=payload, headers=self.headers, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if j.get("status") == "ok":
                    self.session = j.get("session")
                    print(f"✓ Created FlareSolverr session: {self.session}")
                    return True
        except Exception as e:
            print(f"✗ Failed to create FlareSolverr session: {e}")
        return False

    def destroy_session(self):
        if not self.session:
            return
        payload = {"cmd": "sessions.destroy", "session": self.session}
        try:
            requests.post(self.api_url, json=payload, headers=self.headers, timeout=10)
            print(f"✓ Destroyed FlareSolverr session: {self.session}")
        except Exception:
            pass
        finally:
            self.session = None

    def fetch(self, url, max_retries=3, max_timeout_ms=60000):
        """
        Fetch the page HTML through FlareSolverr.
        Returns HTML string or None on failure.
        """
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": max_timeout_ms
            }
            if self.session:
                payload["session"] = self.session

            try:
                r = requests.post(self.api_url, json=payload, headers=self.headers, timeout=self.timeout)
                if r.status_code == 200:
                    j = r.json()
                    if j.get("status") == "ok":
                        html = j.get("solution", {}).get("response", "")
                        if html:
                            return html
                        else:
                            print(f"  ✗ FlareSolverr returned empty content for {url}")
                    else:
                        print(f"  ✗ FlareSolverr error (attempt {attempt}): {j.get('message')}")
                else:
                    print(f"  ✗ HTTP {r.status_code} from FlareSolverr (attempt {attempt})")
            except RequestException as e:
                print(f"  ✗ Request exception (attempt {attempt}): {e}")
            except Exception as e:
                print(f"  ✗ Unexpected error (attempt {attempt}): {e}")

            if attempt < max_retries:
                wait = random.uniform(5, 12)
                print(f"  Waiting {wait:.1f}s before retry...")
                time.sleep(wait)
        return None

# -------------------------
# Page parsing logic (adapted from original scrape_post.py)
# -------------------------
def parse_post_page(html):
    """
    Parse an individual post page HTML and return a dict of extracted fields.
    We expect a <table class="moret"> with <tr><td> pairs (some rows hold two columns of key/value).
    Returns a dict mapping keys to values.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="moret")
    if not table:
        return {}

    data = {}
    rows = table.find_all("tr")
    for tr in rows:
        tds = tr.find_all("td")
        # Many rows have 4 tds: key1, value1, key2, value2
        if len(tds) == 4:
            key1 = tds[0].get_text(strip=True)
            val1 = tds[1].get_text(strip=True)
            key2 = tds[2].get_text(strip=True)
            val2 = tds[3].get_text(strip=True)
            if key1:
                data[key1] = val1
            if key2:
                data[key2] = val2
        # Some pages may have 2 tds: key, value
        elif len(tds) == 2:
            key = tds[0].get_text(strip=True)
            val = tds[1].get_text(strip=True)
            if key:
                data[key] = val
        else:
            # skip unexpected structures
            continue

    return data

# -------------------------
# Main script
# -------------------------
def main():
    # Config
    flaresolverr_url = "http://localhost:8191/v1"
    input_csv_candidates = [
        "hrefs_list_flaresolverr.csv"   # expected
    ]
    output_csv = "extracted_table_data.csv"
    # Columns to write (you can extend this if needed)
    out_columns = ['Date', 'Contact', 'Price', 'Make', 'Model', 'YOM', 'Mileage (km)', 'Gear', 'Fuel Type', 'Options', 'Engine (cc)']

    # Load input CSV (try candidates)
    df = None
    for name in input_csv_candidates:
        try:
            df = pd.read_csv(name)
            print(f"✓ Loaded input CSV: {name} (rows: {len(df)})")
            break
        except FileNotFoundError:
            continue
        except Exception as e:
            print(f"✗ Error reading {name}: {e}")
            continue

    if df is None:
        print("✗ Could not find input CSV. Make sure one of the expected filenames exists:")
        for name in input_csv_candidates:
            print(f"  - {name}")
        return

    # Ensure required columns exist
    if 'URL' not in df.columns:
        print("✗ Input CSV must have a 'URL' column.")
        return

    # Initialize FlareSolverr client
    try:
        client = FlareSolverrClient(flaresolverr_url)
    except Exception as e:
        print(f"✗ FlareSolverr client init failed: {e}")
        return

    # Create session (optional but helps with cookies)
    if not client.create_session():
        print("! Warning: could not create FlareSolverr session. Proceeding without a persistent session.")

    # Open output CSV and process rows
    try:
        with open(output_csv, mode='w', newline='', encoding='utf-8') as fout:
            writer = csv.writer(fout)
            writer.writerow(out_columns)

            total = len(df)
            for idx, row in df.iterrows():
                url = row.get('URL') or row.get('Url') or row.get('url')
                date = row.get('DATE') or row.get('Date') or row.get('date', '')

                if not url or pd.isna(url):
                    print(f"[{idx+1}/{total}] Skipping empty URL")
                    continue

                print(f"[{idx+1}/{total}] Fetching: {url}")

                html = client.fetch(url, max_retries=3)
                if html is None:
                    print(f"  ✗ Failed to fetch {url} — skipping")
                    # optional: write a placeholder line or continue
                    continue

                data = parse_post_page(html)
                if not data:
                    print("  ⚠ No table data found on page")
                    # write empty row with date if you want to keep index
                    writer.writerow([date] + [''] * (len(out_columns) - 1))
                else:
                    writer.writerow([
                        date,
                        data.get('Contact', ''),
                        data.get('Price', ''),
                        data.get('Make', ''),
                        data.get('Model', ''),
                        data.get('YOM', ''),
                        data.get('Mileage (km)', '') or data.get('Mileage', ''),
                        data.get('Gear', ''),
                        data.get('Fuel Type', ''),
                        data.get('Options', ''),
                        data.get('Engine (cc)', '') or data.get('Engine', '')
                    ])
                    print("  ✓ Extracted and saved")

                # polite randomized delay between requests
                delay = random.uniform(2.5, 6.5)
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error while writing output: {e}")
    finally:
        client.destroy_session()
        print(f"\n✓ Finished. Output saved to: {output_csv}")

if __name__ == "__main__":
    main()
