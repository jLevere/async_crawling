import asyncio
import aiohttp
import csv
import sys
import re
import time
import random

from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup

'''
    Demonstrates using queues with asyncio
'''


async def runner(name, queue):
    while True:
        sleep_for = await queue.get()

        await asyncio.sleep(sleep_for)

        queue.task_done()

        print(f'{name} has slept for {sleep_for:.2f} seconds')



async def main():

    queue = asyncio.Queue()

    total_sleep_time = 0
    for _ in range(20):
        sleep_for = random.uniform(0.05, 1.0)
        total_sleep_time += sleep_for
        queue.put_nowait(sleep_for)

    tasks = []
    for i in range(3):
        task = asyncio.create_task(runner(f'runner-{i}', queue))
        tasks.append(task)

    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    for task in tasks:
        task.cancel()
    
    print('=========================================')
    print(f'3 workers slepf in paralle for {total_slept_for:.2f} seconds')
    print(f'total expected sleep time: {total_sleep_time:.2f} seconds')


if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

asyncio.run(main())