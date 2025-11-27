import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
from tqdm import tqdm 

links=[]
page=1

isHaveNextPage=True

while(isHaveNextPage):

    target_url=f'https://ikman.lk/en/ads/sri-lanka/three-wheelers/bajaj?sort=date&order=desc&buy_now=0&urgent=0&page={page}&tree.brand=bajaj'
    url=requests.get(target_url)
    soup=BeautifulSoup(url.text,'lxml')
    product=soup.find('ul',class_="list--3NxGO")

    for item in tqdm(product.find_all("li", class_="normal--2QYVk gtm-normal-ad")):
        anchor = item.find('a')
        if anchor is not None:
            link = anchor.get('href')
            link=f"https://ikman.lk{link}"
            links.append(link)
    
        else:
            print('No anchor tag found in the item.')
    if page==66:
        isHaveNextPage=False
    page+=1

# Convert the list of links into a DataFrame
df = pd.DataFrame(links, columns=['Links'])
df.to_csv("threewheel_2025-04-04.csv")
