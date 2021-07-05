import asyncio, aiohttp, aiohttp_socks
import csv, time, sys, re, traceback

from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer


'''
    uses split workers and queues as detailed in the first version.  outputs in csv format.
'''

def line_count(infile):
    with open(infile) as f:
        for i, _ in enumerate(f):
            pass
    return i

'''
    producers run a set number of times and the consumers run till the queue gets to compleation
'''

async def runner_link(links, session, sem):
    seen_urls = []

    for _ in range(2):
        url = await links.get()
        

        print(f'{asyncio.current_task()} get')
        try:
            async with sem, session.get(url, timeout=10) as r:
                text = await r.content.read(-1)
            
            soup = BeautifulSoup(text, 'lxml', parse_only=SoupStrainer('a'))

            for tag in soup.find_all('a'):
                link = tag.get('href')

                if link and link[0:4] == 'http' and link not in seen_urls:
                    seen_urls.append(link)
                    await links.put(link)

    
        except Exception as e:
            traceback.print_exc()
            print(f'qsize: {links.qsize()} link runner error: ' + str(e))

        finally:
            links.task_done()


async def runner_title(links, results, session, sem):

    #await asyncio.sleep(4)

    while True:

        url = await links.get()
        

        #print(f'{url} being run')

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

            #await links.task_done()
        
        finally:
            await results.put(response)
            links.task_done()
            
                

async def main():

    # set up csv reader
    infile = 'tiney_urls.csv'
    reader = csv.reader(open(infile, 'r'))

    # links holds the urls and results holds the tittles and stuff
    links = asyncio.Queue()
    results = asyncio.Queue()

    # load the seed urls into the queue
    for url in reader:
        links.put_nowait(url[0])

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
            task_link = asyncio.create_task(runner_link(links, session, sem), name=f'link_runner-{_}')
            producers.append(task_link)

        consumers = []
        for _ in range(30):
            task_title = asyncio.create_task(runner_title(links, results, session, sem), name=f'title_runner-{_}')
            consumers.append(task_title)

        await asyncio.gather(*producers)
        
        print(f'producers are finished, qsize: {links.qsize()}')

        #await asyncio.gather(*consumers)
        await links.join()

        print('consumers are joined')

        for task in consumers:
            print('cancelled consumer')
            task.cancel()



    title_out_writer = csv.writer(open('title_out.csv', 'w+', newline=''))


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


    print('========= stats ==========')
    print(f'seed urls: {line_count(infile)}')
    print(f'titles seen: {count_titles}')
    print(f'urls still in queue: {links.qsize()}')


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