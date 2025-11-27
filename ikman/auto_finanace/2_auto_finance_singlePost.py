import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm 
import time
import re
import csv
import os
from datetime import datetime, timedelta

path=os.path.join("ikman_auto_finance_post_links_2025-11-20_14-09.csv")

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')  # e.g., '2025-11-20_13-52'
path_postdata = f'ikman_auto_finance_post_data_{timestamp}.csv'

df=pd.read_csv(path)

# Define column headers
fieldnames = [
    "Post_Link", "Posted_Title", "Posted_Date", "Brand", "Model", "Grade",
    "Trim / Edition", "Year of Manufacture", "Condition", "Transmission",
    "Body type", "Fuel type", "Engine capacity", "Mileage", "Vehicle_Price"
]

# Open the file in append mode to write each record immediately
with open(path_postdata, mode='a', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    # Check if the file is empty, and write the header only once
    if file.tell() == 0:
        writer.writeheader()

    # Iterate over the links
    for link in tqdm(df['Link'][2566:]):
        
        car_info = {
            "Post_Link": "0",
            "Posted_Title": "0",
            "Posted_Date": "0",
            "Brand": "0",
            "Model": "0",
            "Grade": "0",
            "Trim / Edition": "0",
            "Year of Manufacture": "0",
            "Condition": "0",
            "Transmission": "0",
            "Body type": "0",
            "Fuel type": "0",
            "Engine capacity": "0",
            "Mileage": "0",
            "Vehicle_Price": "0"
        }

        try:
            headers = {
                "User-Agent": "web-scrapping",
                "From": "youremail@example.com"
            }

            car_info['Post_Link'] = link
            response = requests.get(link, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract the header and title
            header = soup.find("div", class_='title-wrapper--1lwSc')
            posted_title_name = header.find('h1').get_text() if header and header.find('h1') else ""
            car_info['Posted_Title'] = posted_title_name

            posted_date = header.find("div", class_='subtitle-wrapper--1M5Mv') if header else None
            posted_date_str = re.sub('<[^<]+?>', '', str(posted_date)) if posted_date else ""
            car_info['Posted_Date'] = posted_date_str

            # Extract the vehicle price
            vehicle_price = soup.find("div", class_='amount--3NTpl')
            price = vehicle_price.get_text() if vehicle_price else ""
            car_info['Vehicle_Price'] = price

            # Extract vehicle details
            vehicle_details = soup.find("div", class_='ad-meta--17Bqm')
            vehicle_details_table_rows = vehicle_details.find_all("div", class_='full-width--XovDn') if vehicle_details else []

            for row in vehicle_details_table_rows:
                tag = row.find('div', class_='word-break--2nyVq label--3oVZK')
                label = tag.get_text() if tag else ""
                tag1 = row.find('div', class_='word-break--2nyVq value--1lKHt')
                value = tag1.get_text() if tag1 else ""
                car_info[label.strip("' :")] = value

            # Write the current record to the CSV file
            writer.writerow(car_info)

        except Exception as e:
            print(f"Error while processing {link}: {e}")

        time.sleep(0.25)







 
          
        
    
    


