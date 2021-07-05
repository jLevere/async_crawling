from bs4 import BeautifulSoup
from bs4.element import SoupStrainer
import requests
from urllib.parse import urlsplit, urljoin



def parse_links(text):
    links = []
    # parse only for anchor tags
    soup = BeautifulSoup(text, 'lxml', parse_only=SoupStrainer('a'))
    # get all the anchor tags
    for tag in soup.find_all('a'):
        # get the href tag from the anchor
        url = tag.get('href')
        # if its not empty, starts with 'http' and hasnt been seen before,
        if len(url) != 0 and url[0:4] == 'http' and url not in links:
            # add to the list
            links.append(url)

    # if no links were loaded into list, fill with message
    if len(links) == 0:
        links.append('no links on page')

    return links

    

path = 'http://google.com'
r = requests.get(path)

links = parse_links(r.text)

for link in links:
    print(link)