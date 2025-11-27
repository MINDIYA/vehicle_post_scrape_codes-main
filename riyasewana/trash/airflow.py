
import requests
from bs4 import BeautifulSoup

def scrape_sports():
    website = "https://www.thesun.co.uk/sport/football/"
    page = requests.get(website)
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.find_all("div", class_="teaser__copy-container")

    sports_data = []
    for element in elements:
        # title_element = element.find("h3", class_="teaser__headline")
        # subtitle_element = element.find("p", class_="teaser__subdeck")
        link_element = element.find("a", class_="text-anchor-wrap")

        # print(title_element)
        # print(subtitle_element)
        # print(link_element)
        # print('---------')

        if link_element is not None:
            link = link_element['href']
            sports_data.append((link))

    print(sports_data)


if __name__=="__main__":
    scrape_sports()
