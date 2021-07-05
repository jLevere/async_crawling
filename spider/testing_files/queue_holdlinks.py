import asyncio, aiohttp, aiohttp_socks
import csv, time, sys

from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

def line_count(infile):
    with open(infile) as f:
        for i, l in enumerate(f):
            pass
    return i

async def runner(url, session, sem, queue):
    response = {}
    response['url'] = url.strip()

    try:
        async with sem, session.get(url, timeout=7) as r:
            text = await r.content.read(-1)
            status = r.status
        response['links'] = await parse_links(text)
        response['status'] = status

        for link in response['links']:
            await queue.put(link)

        return response
    except Exception as e:
        if len(str(e)) == 0:
            response['error'] = e.__class__.__name__
        else:
            response['error'] = str(e)
        
        return response
    

async def parse_links(text):
    links = []
    soup = BeautifulSoup(text, 'lxml', parse_only=SoupStrainer('a'))

    for tag in soup.find_all('a'):
        url = urlsplit(tag.get('href'))[1]
        if len(url) != 0 and url not in links:
            links.append(url)

    if len(links) == 0:
        links.append('no links on page')

    return links


async def main():

    infile = 'small_urls.csv'
    
    reader = csv.reader(open(infile, 'r'))
    
    queue = asyncio.Queue()

    sem = asyncio.Semaphore(100)
    conn = aiohttp_socks.ProxyConnector.from_url('socks5://10.64.0.1:1080')

    headers = {
        'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/89.0',
        'Accept' : 'text/*',
        'DNT' : '1'
    }

    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        tasks = []
        for url in reader:
            task = asyncio.create_task(runner(url[0], session, sem, queue))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)

    errors = 0
    for result in results:
        if 'error' in result:
            print(result)
            errors += 1
    
    links = []
    for i in range(queue.qsize()):
        link = queue.get_nowait()
        link = 'http://' + link
        if link not in links and link != 'http://no links on page':
            links.append(link)

    out_writer = csv.writer(open('new_urls.csv', 'w+', newline=''))

    link_count = 0
    for link in links:
        out_writer.writerow([link])
        print(link)
        link_count += 1
        
    
    print('========= stats ==========')
    print(f'urls: {line_count(infile)}')
    print(f'errors: {errors}')
    print(f'{link_count}')

if __name__ == "__main__":

    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()
    asyncio.run(main(), debug=True)

    print("---------total time: {}".format(time.time() - start_time))