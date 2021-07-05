import asyncio, aiohttp, aiohttp_socks # async imports
import csv, time, sys, re, traceback, json # utility imports

from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

def line_count(infile):
    with open(infile) as f:
        for i, _ in enumerate(f):
            pass
    return i+1

'''
    Uses a producer consumer structure to recursivly extract links and titles from webpages.

    TODO:
        Needs better loggging and error handleing.  
        Transition away from using a queue for the results for cleaness and preformance.
        The path it takes is somewhat unpredicable as well.
'''

async def producer(index, links, session, sem):
    seen_urls = []

    for recur in range(3):
        url = (await index.get())[1] # [1] is to get the url not priorty from the queue
        
        #print(f'{asyncio.current_task()} get') # debugging, shows general activity

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
                    # put back into producer queue to be checked on next run.  use the current recursion/loop number as priority to ensure predictable coverage
                    await index.put([recur, link])

    
        except Exception as e:
            #traceback.print_exc() # debugging
            print(f'qsize: {links.qsize()} producer error: ' + e.__class__.__name__)



async def consumer(links, results, session, sem):

    while True:

        url = await links.get()

        timeout = 14

        response = {
            'url' : url,
            'get timeout' : timeout
        }

        try:
            async with sem, session.get(url, timeout=timeout) as r:
                text = await r.content.read(-1)
                status = r.status
                redirects = len(r.history)
            
            soup = BeautifulSoup(text, 'lxml')
            if soup.title:
                title = soup.title.string
                title = re.sub(r'\W+', ' ', title)
            else:
                title = 'no title on page'

            response['status'] = status
            response['title'] = title
            response['redirects'] = redirects


        except Exception as e:

            print(f'link_queue: {links.qsize()} results_queue: {results.qsize()} consumer runner err: {e.__class__.__name__}')

            if len(str(e)) == 0:
                response['error'] = e.__class__.__name__
            else:
                response['error'] = str(e)
        
        finally:
            await results.put(response)
            # this makes sure that the queue gets updated on compleation status
            links.task_done()
            
                

async def main():

    # set up csv reader
    infile = '.seed_urls.csv'
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
        
        #print(f'producers are finished, qsize: {links.qsize()}') # debugging

        await links.join()

        #print('consumers are joined') # debugging

        for task in consumers:
            task.cancel()

    print('========= stats ==========')
    print(f'seed urls: {line_count(infile)}')
    print(f'titles seen: {results.qsize()}')
    print(f'urls still in links queue: {links.qsize()}')
    print(f'urls still in the index queue: {index.qsize()}')

    # write the titles to file in json
    errors = 0
    redirs = 0
    redir_avg = 0
    with open('.title_out.json', 'w+', encoding='utf-8') as f:
        for _ in range(results.qsize()):
            entry = results.get_nowait()
            json_object = json.dumps(entry, ensure_ascii=False)
            f.write(json_object + '\n')

            if 'error' in entry:
                errors += 1
            if 'redirects' in entry and entry['redirects'] != 0:
                redirs += 1
                redir_avg = entry['redirects']
    
    if redirs != 0 and redir_avg != 0:
        redir_avg = redir_avg / redirs
    else:
        redir_avg = -1

    # dump the contents of the index queue at finish.  this also shows their priority.
    # there will most likely still be items in it.
    with open('.urls_out.log', 'w+', encoding='utf-8') as f:
        for _ in range(index.qsize()):
            f.write(str(index.get_nowait()) + '\n')

    # more stats
    print(f'errors getting titles: {errors}')
    print(f'average redirects: {redir_avg}')


if __name__ == "__main__":

    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        traceback.print_exc()
        print('keyboard interupt was used')

    print("---------total time: {}".format(time.time() - start_time))