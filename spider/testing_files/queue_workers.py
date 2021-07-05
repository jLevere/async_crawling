import asyncio, aiohttp, aiohttp_socks
import csv, time, sys, re

from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

'''
Structure:
            runner   _  # gets the pages and passes to proccessors
               |     |
               |    procces links 
               |    procces title
               |
              main  # starts a list of runners who each draw from the link queue


Notes:

    Because there are so many links on pages, it would prolly be better to seperate the 
runner to two parts, one for the title and one for links.  This would allow the title runner 
to have a chance at keeping pace with the link runner.  it would also speed up the proccess as 
it is kinda slow for async rn.
'''

def line_count(infile):
    with open(infile) as f:
        for i, l in enumerate(f):
            pass
    return i

async def runner(links, results, session, sem):
    seen_urls = []
    add_linky = True

    # insted of running in range, should prolly just run until queue is empty or something.  
    # and use the upper limit stop on the queue to stop over growth

    while not links.empty():

        url = await links.get()


        # set up dictionary for the results
        response = {}
        response['url'] = url

        try:
            async with sem, session.get(url, timeout=7) as r:
                text = await r.content.read(-1)
                status = r.status

            # set up normal response dict
            response['status'] = status
            response['title'] = await process_title(text)

            # get a list of the urls on page
            urls = await process_links(text)

            # cut off check for adding to queue
            if links.qsize() > 500:
                print(f'task: {asyncio.current_task()} hit link limit <==================')
                add_linky = False

            # if there are urls on the page, put them into queue.
            # check if the runner has seen the url first
            if 'no links on page' not in urls and add_linky:
                for url in urls:
                    url = 'http://' + url
                    if url not in seen_urls:
                        seen_urls.append(url)
                        await links.put(url)

        except Exception as e:
            print(f'{url} {str(e)}')
            if len(str(e)) == 0:
                response['error'] = e.__class__.__name__
            else:
                response['error'] = str(e)
        
        finally:
            # add the results to the queue, with the goal of including exception responses too
            await results.put(response)


async def process_title(text):
    soup = BeautifulSoup(text, 'lxml')
    if soup.title:
        title = soup.title.string
        # remove specal chars like newlines
        title = re.sub(r'\W+', ' ', title)
    else:
        title = 'no title on page'
    return title


async def process_links(text):
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

    # start up three runners that will run for 100 runs each
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        tasks = []
        for _ in range(200):
            task = asyncio.create_task(runner(links, results, session, sem))
            tasks.append(task)

        # gather and close the runners
        await asyncio.gather(*tasks)

    title_out_writer = csv.writer(open('title_out.csv', 'w+', newline=''))

    # proccess the titles
    count_titles = 0
    for _ in range(results.qsize()):
        count_titles += 1
        entry = results.get_nowait()
        try:
            title_out_writer.writerow([entry])
        except Exception as e:
            # if an exception in writing, encode the string and try to write again
            entry['title'] = entry['title'].encode('utf-8')
            print(f'titlewriter fail: {str(e)} for {entry}')
            title_out_writer.writerow([entry])


    print('========= stats ==========')
    print(f'urls: {line_count(infile)}')
    print(f'titles seen: {count_titles}')
    print(f'urls still in queue: {links.qsize()}')


if __name__ == "__main__":

    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()

    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        print('keyboard interupt was used')

    print("---------total time: {}".format(time.time() - start_time))