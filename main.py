import asyncio
from aiohttp import ClientSession
import uvloop
import os
import json

MAX_CONCURRENT=100
MAX_SIZE=100



async def download_file(session: ClientSession, semaphore:asyncio.Semaphore, remote_url:str, local_filename:str):
    async with semaphore:
        async with session.get(remote_url) as response:
            contents =  await response.read()

            with open(local_filename, 'wb') as f:
                f.write(contents)
            return local_filename


async def iter_all_files(session:ClientSession, list_url:str):
    async with session.get(list_url) as response:
        if response.status != 200:
            raise RuntimeError(f" Bad status code: {response.status}")
        content = json.loads(await response.read())
    while True:
        for filename in content['FileNames']:
            yield filename
        if 'NextFile' not in content:
            return
        next_page_url = f'{list_url}?next-marker={content["NextFile"]}'
        async with session.get(next_page_url) as response:
            if responsestatus != 200:
                raise RuntimeError(f" Bad status code: {response.status}")
            content = json.loads(await response.read())


async def download_files(host: str, port:int, outdir: int)-> None:
    hostname = f'{host}:{port}'
    list_url = f'{hostname}/list'
    semaphore = asyncio.Semaphore(MAX_CONCURRENT) # control how many tasks are running
    task_queue = asyncio.Queue(MAX_SIZE) # only has a certain amount of task at any given time
    asyncio.create_task(result_worker(task_queue)) 
    async with ClientSession() as session:
        async for filename in iter_all_files(session, list_url):
            remote_url = f'{get_url}/{filename}'
            task = asyncio.create_task(download_file(session, semaphore, remote_url,
                                                     os.path.join(outdir, filename))
                                                     )
            await task_queue.put(task)
        