import asyncio, aiohttp, aiohttp_socks
import csv, time, sys, re, traceback

from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

def line_count(infile):
    with open(infile) as f:
        for i, _ in enumerate(f):
            pass
    return i

'''
    TODO:
        Needs better loggging and error handleing.  
        Maybe a better file output format would be nice as well.  csv just is inflexable and unclear to read.
        Transition away from using a queue for the results for simplity and preformance.
'''

async def producer(index, links, session, sem):
    seen_urls = []

    for recur in range(5):
        url = await index.get()
        url = url[1]
        
        print(f'{asyncio.current_task()} get')
        try:
            async with sem, session.get(url, timeout=10) as r:
                text = await r.content.read(-1)
            
            soup = BeautifulSoup(text, 'lxml', parse_only=SoupStrainer('a'))
            for tag in soup.find_all('a'):
                link = tag.get('href')

                if link and link[0:4] == 'http' and link not in seen_urls:

                    seen_urls.append(link)
                    # put into queue for the consumer
                    await links.put(link)
                    # put back into producer queue to be checked on next run.  use the current recursion/loop number as priority to ensure good coverage
                    await index.put([recur, link])

    
        except Exception as e:
            traceback.print_exc()
            print(f'qsize: {links.qsize()} link runner error: ' + str(e))



async def consumer(links, results, session, sem):

    while True:

        url = await links.get()

        if links.qsize() % 100 == 0:
            print(f'current link queue: {links.qsize()}')

        response = {}
        response['url'] = url
        print(f'{asyncio.current_task()} get')
        try:
            async with sem, session.get(url, timeout=7) as r:
                text = await r.content.read(-1)
                status = r.status
            
            soup = BeautifulSoup(text, 'lxml')
            if soup.title:
                title = soup.title.string
                title = re.sub(r'\W+', ' ', title)
            else:
                title = 'no title on page'

            response['status'] = status
            response['title'] = title


        except Exception as e:
            
            print(f'lqueue: {links.qsize()} rqueue: {results.qsize()} title runner err: {str(e)}')

            if len(str(e)) == 0:
                traceback.print_exc()
                response['error'] = e.__class__.__name__
            else:
                response['error'] = str(e)
        
        finally:
            await results.put(response)
            links.task_done()
            
                

async def main():

    # set up csv reader
    infile = 'tiney_urls.csv'
    reader = csv.reader(open(infile, 'r'))

    '''
    index:
         holds the agregate urls in a priority queue based on the recursion of the page.
    links:
         holds a blob of all found urls that the consumer pulls from
    results:
         holds the output from the consumer
    '''
    index = asyncio.PriorityQueue()
    links = asyncio.Queue()
    results = asyncio.Queue()

    # load the seed urls into the queue with priority 0
    for url in reader:
        index.put_nowait([0, url[0]])

    # setup the normal connection details
    sem = asyncio.Semaphore(500)
    conn = aiohttp_socks.ProxyConnector.from_url('socks5://10.64.0.1:1080')

    headers = {
        'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/89.0',
        'Accept' : 'text/*',
        'DNT' : '1'
    }

    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        producers = []
        for _ in range(5):
            task_link = asyncio.create_task(producer(index, links, session, sem), name=f'producer-{_}')
            producers.append(task_link)

        consumers = []
        for _ in range(200):
            task_title = asyncio.create_task(consumer(links, results, session, sem), name=f'consumer-{_}')
            consumers.append(task_title)

        await asyncio.gather(*producers)
        
        print(f'producers are finished, qsize: {links.qsize()}')

        await links.join()

        print('consumers are joined')

        for task in consumers:
            task.cancel()



    title_out_writer = csv.writer(open('title_out.csv', 'w+', newline=''))
    url_out_writer = csv.writer(open('url_out.csv', 'w+', newline=''))

    # proccess the titles
    count_titles = results.qsize()

    for _ in range(results.qsize()):
        entry = results.get_nowait()
        try:
            title_out_writer.writerow([entry])
        except Exception as e:
            # if an exception in writing, encode the string and try to write again
            entry['title'] = entry['title'].encode('utf-8')
            print(f'titlewriter fail: {str(e)} for {entry}')
            title_out_writer.writerow([entry])

    # print the urls remaining in the index queue.  this also shows their priority.
    index_count = index.qsize()
    for _ in range(index.qsize()):
        entry = index.get_nowait()
        url_out_writer.writerow([entry])
        
    print('========= stats ==========')
    print(f'seed urls: {line_count(infile)}')
    print(f'titles seen: {count_titles}')
    print(f'urls still in links queue: {links.qsize()}')
    print(f'urls still in the index queue: {index_count}')


if __name__ == "__main__":

    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()

    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        traceback.print_exc()
        print('keyboard interupt was used')

    print("---------total time: {}".format(time.time() - start_time))