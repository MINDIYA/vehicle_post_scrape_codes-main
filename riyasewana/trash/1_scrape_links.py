import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import random
import csv

# Set up Selenium
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run headless for no UI
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

makes = ['nissan', 'suzuki', 'micro', 'mitsubishi', 'mahindra', 'mazda', 'daihatsu', 'hyundai', 'kia', 'bmw', 'perodua', 'tata']
types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
# makes = ['suzuki']

page = 1

total_requests = len(makes) * len(types) * 10

# Open the CSV file for writing
with open('hrefs_list.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['DATE', 'MAKE', 'TYPE', 'URL','PAGE'])  # Write header

    with tqdm(total=total_requests, desc="Scraping Progress") as pbar:
        for make in makes:
            for type in types:
                isHaveNextPage = True
                while isHaveNextPage:
                    if page == 1:
                        target_url = f'https://riyasewana.com/search/{type}/{make}'
                    else:
                        target_url = f'https://riyasewana.com/search/{type}/{make}?page={page}'

                    driver.get(target_url)

                    time.sleep(random.uniform(3, 7))  # Random delay between 5 to 10 seconds

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    content_div = soup.find('div', id='content')

                    print(soup)

                    pagination = soup.find('div', class_='pagination')

                    if pagination is None:
                        current_page = 1
                    else:
                        current_page = pagination.find('a', class_="current").text.strip()

                    current_page_number = current_page

                    if not int(current_page_number) == page:
                        print(f"{make}:{type}:{page}")
                        isHaveNextPage = False
                        page = 1
                        print("page is no content-----------------")
                        break

                    ul_tag = content_div.find('ul')

                    if ul_tag is None or len(ul_tag.find_all('li', class_="item round")) == 0:
                        print(f"{make}:{type}:{page}")
                        isHaveNextPage = False
                        page = 1
                        print("page is no content-----------------")
                        break

                    li_tags = ul_tag.find_all('li', class_="item round") if ul_tag else []

                    for li in li_tags:
                        h2_tag = li.find('h2')
                        date_div = li.find('div', class_='boxintxt s')
                        date_text = date_div.text.strip() if date_div else ""

                        try:
                            date_obj = pd.to_datetime(date_text)
                            if date_obj < pd.Timestamp('2025-11-01'):
                                isHaveNextPage = False
                                break
                        except ValueError:
                            print(f"Error parsing date: {date_text}")

                        if h2_tag:
                            a_tag = h2_tag.find('a')
                            if a_tag and 'href' in a_tag.attrs:
                                # Write each record to the CSV file
                                writer.writerow([date_text, make, type, a_tag['href'],page])

                    page += 1
                    pbar.update(1)  # Update the progress bar

# Quit the driver
driver.quit()

# Print confirmation message
print("The hrefs have been successfully saved to hrefs_list.csv.")
