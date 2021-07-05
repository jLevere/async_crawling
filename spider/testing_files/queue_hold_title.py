import asyncio, aiohttp, aiohttp_socks
import re, csv, time, sys

from bs4 import BeautifulSoup

'''
    using async queues to hold the results of async functions
'''


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
        response['title'] = await parse_title(text)
        response['status'] = status

        await queue.put(response)

        return response
    except Exception as e:
        if len(str(e)) == 0:
            response['error'] = e.__class__.__name__
        else:
            response['error'] = str(e)
        
        return response
    

async def parse_title(text):
    soup = BeautifulSoup(text, 'lxml')
    if soup.title:
        title = soup.title.string
        title = re.sub(r'\W+', ' ', title)
    else:
        title = "no title on page"
    return title


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
    
    for i in range(queue.qsize()):
        print(f'{i + 1}st: {queue.get_nowait()}')

    print('========= stats ==========')
    print(f'urls: {line_count(infile)}')
    print(f'errors: {errors}')

if __name__ == "__main__":

    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()
    asyncio.run(main(), debug=True)

    print("---------total time: {}".format(time.time() - start_time))