from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm  # Progress bar
import time
import csv
import os

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')  # e.g., '2025-11-20_13-52'
filename = f'ikman_auto_finance_post_links_{timestamp}.csv'


headers = {
    "User-Agent": "web-scrapping",
    "From": "youremail@example.com"  # Include your email so the website owner can contact you if necessary
}

fieldnames = ["Link", "Page", "Transmission", "Fuel_Type"]

# Open a file in append mode to save each record
with open(filename, mode="a", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    # Check if the file is empty and write the header only once
    if file.tell() == 0:
        writer.writeheader()

    page = 1
    # make = ['suzuki']
    make = ['honda', 'toyota', 'nissan', 'suzuki', 'micro', 'mitsubishi', 'mahindra', 'mazda', 'daihatsu', 'hyundai', 'kia', 'bmw', 'perodua', 'tata']
    transmission = ['automatic', 'manual', 'tiptronic']
    fuel_type = ['petrol', 'diesel', 'hybrid', 'electric']

    # Add an outer progress bar for "make"
    for m in tqdm(make, desc="Processing Makes", unit="make"):
        for t in tqdm(transmission, desc=f"Processing Transmissions ({m})", leave=False, unit="transmission"):
            for f in tqdm(fuel_type, desc=f"Processing Fuel Types ({m}, {t})", leave=False, unit="fuel type"):
                isHaveNextPage = True
                while isHaveNextPage:
                    target_url = f'https://ikman.lk/en/ads/sri-lanka/cars/{m}?sort=date&order=desc&buy_now=0&urgent=0&page={page}&enum.transmission={t}&tree.brand={m}&enum.fuel_type={f}'
                    url = requests.get(target_url, headers=headers)
                    soup = BeautifulSoup(url.text, 'lxml')
                    product = soup.find('ul', class_="list--3NxGO")

                    # If no results found, stop the loop
                    if product is None or len(product.find_all("li", class_="normal--2QYVk gtm-normal-ad")) == 0:
                        isHaveNextPage = False
                        page = 1
                        print("page is no content-----------------")
                        break

                    # Add progress bar for items within a page
                    for item in tqdm(product.find_all("li", class_="normal--2QYVk gtm-normal-ad"), desc="Processing Items", leave=False):
                        anchor = item.find('a')
                        if anchor is not None:
                            link = anchor.get('href')
                            link = f"https://ikman.lk{link}"   
                            # Save the link details immediately to the CSV file
                            writer.writerow({
                                "Link": link,
                                "Page": str(page),
                                "Transmission": t,
                                "Fuel_Type": f
                            })
                        else:
                            print('No anchor tag found in the item.')

                    page += 1
                    time.sleep(0.5)
