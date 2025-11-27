# import time
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# from tqdm import tqdm 

# # Read the CSV file
# df = pd.read_csv('threewheel_2025-03-04.csv')

# # Initialize empty lists
# post_links=[]
# posted_title = []
# posted_date_list = []
# price_list=[]

# make = []
# model = []
# grade = []
# condition = []
# mileage = []
# yom=[]
# # List of lists to iterate over
# lists = [make, model, grade, condition,mileage,yom]

# # Iterate over the links
# for link in tqdm(df['Links']):

    
#     headers = {
#                 "User-Agent": "web-scrapping",
#                 "From": "youremail@example.com"
#             }

#     try:

#         post_links.append(link)
#         # Send a GET request
#         response = requests.get(link,headers=headers)

#         # Create a BeautifulSoup object and specify the parser
#         soup = BeautifulSoup(response.text, 'html.parser')

#         # Find the header
#         header = soup.find("div", class_='title-wrapper--1lwSc')

#         # Extract the title
#         posted_title_name = header.find('h1').get_text() if header and header.find('h1') else ""
#         posted_title.append(posted_title_name)

#         # Extract the posted date
#         posted_date = header.find("div", class_='subtitle-wrapper--1M5Mv') if header else None
#         posted_date_str = re.sub('<[^<]+?>', '', str(posted_date)) if posted_date else ""
#         posted_date_list.append(posted_date_str)
        

#         vehicle_price = soup.find("div", class_='amount--3NTpl')
#         price=vehicle_price.get_text() if vehicle_price else ""

#         price_list.append(price)

#         # Find the vehicle details
#         vehicle_details = soup.find("div", class_='ad-meta--17Bqm justify-content-flex-start--1Xozy align-items-normal--vaTgD flex-wrap-wrap--2PCx8 flex-direction-row--27fh1 flex--3fKk1')
#         vehicle_details_table_rows = vehicle_details.find_all("div", class_='full-width--XovDn justify-content-flex-start--1Xozy align-items-normal--vaTgD flex-wrap-nowrap--3IpfJ flex-direction-row--27fh1 flex--3fKk1') if vehicle_details else []

#         # Initialize a temporary list with None values
#         temp_list = [None] * len(lists)

#         # Iterate over the rows and append the text to the corresponding list
#         for i, row in enumerate(vehicle_details_table_rows):
#             tag = row.find('div', class_='word-break--2nyVq value--1lKHt')
#             text = tag.get_text() if tag else ""
#             temp_list[i % len(lists)] = text

#         # Append the values from the temporary list to the main lists
#         for i, lst in enumerate(lists):
#             lst.append(temp_list[i])


#         time.sleep(0.25) 

#     except Exception as e:
#         print("something wrong")

# # Create a DataFrame
# df = pd.DataFrame({
#     "Post_Link":post_links,
#     "Posted Title": posted_title,
#     "Posted Date": posted_date_list,
#     "Make": make,
#     "Model": model,
#     "Grade": grade,
#     "Year": yom,
#     "Condition": condition,
#     "Mileage": mileage,
#     "Vehicle Price":price_list
# })

# # Save the DataFrame to a CSV file
# df.to_csv('threewheel_data_2025_03_04.csv', index=False)

import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from tqdm import tqdm
import csv

# Read the CSV file
df = pd.read_csv('threewheel_2025-03-04.csv')

# Initialize the CSV file for appending
with open('threewheel_data_2025_04_04.csv', 'a', newline='', encoding='utf-8') as csvfile:
    fieldnames = ["Post_Link", "Posted Title", "Posted Date", "Make", "Model", "Grade", "Year", "Condition", "Mileage", "Vehicle Price"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    # If the file is empty, write the header
    if csvfile.tell() == 0:
        writer.writeheader()

    # Iterate over the links
    for link in tqdm(df['Links']):

        headers = {
                    "User-Agent": "web-scrapping",
                    "From": "youremail@example.com"
                }

        try:
            # Send a GET request
            response = requests.get(link, headers=headers)

            # Create a BeautifulSoup object and specify the parser
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the header
            header = soup.find("div", class_='title-wrapper--1lwSc')

            # Extract the title
            posted_title_name = header.find('h1').get_text() if header and header.find('h1') else ""
            
            # Extract the posted date
            posted_date = header.find("div", class_='subtitle-wrapper--1M5Mv') if header else None
            posted_date_str = re.sub('<[^<]+?>', '', str(posted_date)) if posted_date else ""

            vehicle_price = soup.find("div", class_='amount--3NTpl')
            price = vehicle_price.get_text() if vehicle_price else ""

            # Find the vehicle details
            vehicle_details = soup.find("div", class_='ad-meta--17Bqm justify-content-flex-start--1Xozy align-items-normal--vaTgD flex-wrap-wrap--2PCx8 flex-direction-row--27fh1 flex--3fKk1')
            vehicle_details_table_rows = vehicle_details.find_all("div", class_='full-width--XovDn justify-content-flex-start--1Xozy align-items-normal--vaTgD flex-wrap-nowrap--3IpfJ flex-direction-row--27fh1 flex--3fKk1') if vehicle_details else []

            # Initialize a temporary list with None values
            temp_list = [None] * len(fieldnames[3:])

            # Iterate over the rows and append the text to the corresponding list
            for i, row in enumerate(vehicle_details_table_rows):
                tag = row.find('div', class_='word-break--2nyVq value--1lKHt')
                text = tag.get_text() if tag else ""
                temp_list[i % len(fieldnames[3:])] = text

            # Prepare the record to be written
            record = {
                "Post_Link": link,
                "Posted Title": posted_title_name,
                "Posted Date": posted_date_str,
                "Make": temp_list[0],
                "Model": temp_list[1],
                "Grade": temp_list[2],
                "Year": temp_list[3],
                "Condition": temp_list[4],
                "Mileage": temp_list[5],
                "Vehicle Price": price
            }

            # Write the record to the CSV file
            writer.writerow(record)

            # Add a delay to avoid getting blocked
            time.sleep(0.25)

        except Exception as e:
            print("Something went wrong:", e)


