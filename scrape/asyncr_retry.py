'''
takes a list of urls in csv and scrapes the tittles of each of them and ouputs into file in csv format.

on timeout, will reattempt connections with increasing timeouts a specified number of times.
'''
import asyncio
import csv
import re
import aiohttp
import time

from aiohttp_socks.connector import ProxyConnector
from bs4 import BeautifulSoup
from datetime import datetime


def loadurls():
    urls = []

    f = csv.reader(open('.small_urls.csv', 'r'), delimiter=',') 
    for line in f:
        urls.append(line[0])

    return urls

async def runner(url, session, sem):

    response = {}
    response['url'] = url
    response['datetime'] = (datetime.now()).strftime("%m/%d/%Y %H:%M:%S")
    
    timeout = 7
    retries = 2
    
    for attempt in range(retries):
        try:
            async with sem, session.get(url.strip(), timeout=timeout) as r:
                text = await r.content.read(-1)
                status = r.status
            
            soup = BeautifulSoup(text, 'lxml')
            
            if soup.title:
                response['title'] = re.sub(r'\W+', ' ', soup.title.string)
            else:
                response['title'] = "no title"
            
            response['status'] = str(status)

            if attempt != 0:
                response['retry'] = f'attempt: {attempt} timeout: {timeout}'

            return response
        except Exception as e:
            
            if isinstance(e, asyncio.TimeoutError) and attempt < retries -1:
                
                timeout = timeout + 10
            elif isinstance(e, asyncio.TimeoutError) and attempt == retries -1:
                response['error'] = e.__class__.__name__
                response['title'] = 'client timeout'
                response['status'] = '-1'

                return response

            else:
                response['error'] = e.__class__.__name__
                response['title'] = "N/A"
                response['status'] = '-1'
                
                return response
        
            
async def main():
    
    urls = loadurls()
    total_urls = len(urls)

    proxy = ProxyConnector.from_url('socks5://10.64.0.1:1080')
    sem = asyncio.Semaphore(500)
    headers = {
        "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
    }

    results = []
    async with aiohttp.ClientSession(connector=proxy, headers=headers) as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(runner(url, session, sem))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # stats gathering setup
    errors = 0
    retries = 0
    count = 0

    f = csv.writer(open('.output.csv', 'w+', newline='', encoding='utf-8'))
    for result in results:
        count += 1
        
        f.writerow([result['url'], result['status'], result['datetime'], result['title']])

        if 'error' in result:
            errors = errors +1
        if 'retries' in result:
            retries = retries +1
    
    # display stats
    print(f'-------- stats ----------')
    print(f'starting urls: {total_urls}')
    print(f'titles found: {count}')
    print(f'errors seen: {errors}')
    print(f'retries used: {retries}')

if __name__ == "__main__":
    start_time = time.time()

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

    print(f'---- {time.time() - start_time} seconds to complete------')