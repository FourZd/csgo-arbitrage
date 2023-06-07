from dbo import (
    get_steam_ids,
    get_connection,
    get_cursor,
    insert_steam_ids,
    get_proxies,
    get_async_connection,
    insert_steam_price,
    get_steam_links
)
import asyncio
import requests
import bs4
import time
import re
import numpy
import random
import aiohttp

async def run_period(proxy, period):
    print('running period with proxy {}'.format(proxy), 'total_len', len(period))
    count = 0
    connection = await get_async_connection()
    async with aiohttp.ClientSession() as session:
        for item in period:
            print('Done ', count, 'from', len(period), 'with proxy {}'.format(proxy))
            print('item is {}'.format(item))
            try:
                print('Entering item {}'.format(item))
                await process_item(proxy, item, session, connection)
                count += 1
                print('Processed item {}'.format(item))
            except Exception as e:
                print(e)
        await session.close()
        await connection.close()

# Define a function to process each item
async def process_item(proxy, item, session, connection):
    print(proxy["proxy"])
    while True:
        # Make request with proxy and credentials
        try:
            if 'https' in item:
                async with session.get(item, proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy']}") as response:
                    response_text = await response.text()
                    response_code = response.status
                await asyncio.sleep(random.uniform(4.00, 7.00) + random.uniform(0, 1.00) + random.uniform(0,  1.00))
                print('Finished sleeping')
                # Parse response to extract item ID
                if response_code == 429:
                    print(f"{item} is rate limited")
                    await asyncio.sleep(random.uniform(10.00, 25.00) + random.uniform(-1.00, 1.00) + random.uniform(-1.00, 1.00))
                    print('Finished sleeping')
                elif response_code == 200:
                    item_id = re.search(
                        r"Market_LoadOrderSpread\(\s*(\d+)\s*\)", response_text
                    )
                    if item_id:
                        item_id = item_id.group(1)
                    else:
                        print("There's no item on market for now")
                        break
                    print('Grouped')
                    item_id_pair = {item: item_id}

                    # Insert item and item ID into database
                    try:
                        print('Inserting results')
                        await insert_steam_ids(connection, item_id_pair)
                        print('Inserted results!')
                    except Exception as e:
                        print(f"Error inserting {item_id_pair} into database: {e}")
                        await asyncio.sleep(3)
                        continue

                    print(f"Inserted item {item} with item ID {item_id}")
                    break
            elif type(item) == int:
                async with session.get(f'https://steamcommunity.com/market/itemordershistogram?country=RU&language=english&currency=23&item_nameid={item}&two_factor=0') as response:
                    response_text = await response.json()
                    response_code = response.status
                await asyncio.sleep(random.uniform(4.00, 7.00) + random.uniform(0, 1.00) + random.uniform(0,  1.00))
                if response_code == 429:
                    print(f"{item} is rate limited")
                    await asyncio.sleep(random.uniform(10.00, 25.00) + random.uniform(-1.00, 1.00) + random.uniform(-1.00, 1.00))
                    print('Finished sleeping')
                elif response_code == 200:
                    print(proxy)
                    autobuy_price = response_text['buy_order_graph'][0][0]
                    print(autobuy_price)
                    await insert_steam_price(connection, item, autobuy_price)
            elif 
        except Exception as e:
            print(f"Error making request to {item}: {e}")
            await asyncio.sleep(3)
            continue
            

async def gather_tasks(items, num_parts, proxies):
    tasks = []
    parts = numpy.array_split(items, num_parts)
    for proxy, part in zip(proxies, parts):
        print(proxy)
        print(len(part))
        tasks.append(asyncio.create_task(run_period(proxy, part)))
    print(tasks)
    await asyncio.gather(*tasks)

async def main():
    # Get proxies and items from database
    connection = get_connection()
    cursor = get_cursor(connection)
    proxies = get_proxies(cursor)

    steam_ids = get_steam_ids(cursor)
    print(steam_ids)
    steam_links = get_steam_links(cursor)
    # Split items between proxies and process them asynchronously
    num_parts = len(proxies)

    if steam_links:
        await gather_tasks(steam_links, num_parts, proxies)
    await asyncio.sleep(10)
    await gather_tasks(steam_ids, num_parts, proxies)

asyncio.run(main())
