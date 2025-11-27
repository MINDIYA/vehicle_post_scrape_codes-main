import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import csv

# Set up Selenium
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run headless for no UI
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Read the CSV file containing URLs
df = pd.read_csv('hrefs_list_flaresolverr', skiprows=range(1, 1300))  # keeps the first row as header

all_data = []


# Open the CSV file for writing
with open('extracted_table_data.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Date','Contact','Price','Make','Model','YOM','Mileage (km)','Gear','Fuel Type','Options','Engine (cc)'])  # Write header
    # Initialize the progress bar for the current group
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing URLs"):
        url = row['URL']
        date = row['DATE']
        # Visit the URL
        driver.get(url)
        
        # Wait for the page to fully load
        time.sleep(3)
        
        # Parse the page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Extract <tr> tags that contain <td> with specific classes
        table = soup.find('table', class_='moret')
        if table:
            rows = table.find_all('tr')
            data = {}
            for row in rows:
                tds = row.find_all('td')
                if len(tds) == 4:
                    key1 = tds[0].text.strip()
                    value1 = tds[1].text.strip()
                    key2 = tds[2].text.strip()
                    value2 = tds[3].text.strip()
                    if key1 and value1:
                        data[key1] = value1
                    if key2 and value2:
                        data[key2] = value2
            
            writer.writerow([
                date,
                data.get('Contact', ''),
                data.get('Price', ''),
                data.get('Make', ''),
                data.get('Model', ''),
                data.get('YOM', ''),
                data.get('Mileage (km)', ''),
                data.get('Gear', ''),
                data.get('Fuel Type', ''),
                data.get('Options', ''),
                data.get('Engine (cc)', '')
            ])

driver.quit()

# Print confirmation message
print("The data has been successfully extracted and saved to make-specific CSV files.")
