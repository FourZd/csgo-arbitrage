from dao import (
    get_connection,
    get_cursor,
    insert_steam_ids,
    get_async_connection,
    update_autobuy_price,
    get_item_data,
    get_bot_credentials,
    update_bot_credentials,
)
from run_selenium import get_steam_cookie
import asyncio
import re
import numpy
import random
import aiohttp
import datetime


async def run_period(proxy, part, cookie=None):
    print("running part with proxy {}".format(proxy), "total_len", len(part))
    count = 0
    connection = await get_async_connection()
    while True:
        async with aiohttp.ClientSession() as session:
            for item in part:
                print("Done ", count, "from", len(part), "with proxy {}".format(proxy))
                print("item is {}".format(item))
                try:
                    print("Entering item {}".format(item))
                    await process_item(proxy, item, session, connection, cookie)
                    count += 1
                    print("Processed item {}".format(item))
                except Exception as e:
                    print(e)
            await session.close()
        await asyncio.sleep(random.uniform(10.00, 25.00) + random.uniform(50.00, 70.00))


# Define a function to process each item
async def process_item(proxy, item, session, connection, cookie):
    print(proxy["proxy"])
    while True:
        # Make request with proxy and credentials
        try:
            if type(item) == str:
                async with session.get(
                    item,
                    proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy']}",
                ) as response:
                    response_text = await response.text()
                    response_code = response.status
                await asyncio.sleep(
                    random.uniform(4.00, 7.00)
                    + random.uniform(0, 1.00)
                    + random.uniform(0, 1.00)
                )
                print("Finished sleeping")
                # Parse response to extract item ID
                if response_code == 429:
                    print(f"{item} is rate limited")
                    await asyncio.sleep(
                        random.uniform(10.00, 25.00)
                        + random.uniform(-1.00, 1.00)
                        + random.uniform(-1.00, 1.00)
                    )
                    print("Finished sleeping")
                elif response_code == 200:
                    item_id = re.search(
                        r"Market_LoadOrderSpread\(\s*(\d+)\s*\)", response_text
                    )
                    if item_id:
                        item_id = item_id.group(1)
                    else:
                        print("There's no item on market for now")
                        break
                    print("Grouped")
                    item_id_pair = {item: item_id}

                    # Insert item and item ID into database
                    try:
                        print("Inserting results")
                        await insert_steam_ids(connection, item_id_pair)
                        print("Inserted results!")
                    except Exception as e:
                        print(f"Error inserting {item_id_pair} into database: {e}")
                        await asyncio.sleep(3)
                        continue

                    print(f"Inserted item {item} with item ID {item_id}")
                    break
            elif type(item) == dict:
                steam_cookies = cookie['cookie']
                item_id = item.get('steam_id')
                item_url = item.get('steam_link')

                if not steam_cookies:
                    raise Exception("No steam cookies found")
                elif not item_url:
                    raise Exception("No item url found")
                else:
                    print(steam_cookies)
                headers = {
                    "Host": "steamcommunity.com",
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "X-Requested-With": "XMLHttpRequest",
                    "Connection": "keep-alive",
                    "Referer": item_url,
                    "Cookie": steam_cookies,
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
                session.headers.update(headers)
                async with session.get(
                    f"https://steamcommunity.com/market/itemordershistogram?country=RU&language=english&currency=23&item_nameid={item_id}&two_factor=0"
                ) as response:
                    response_text = await response.json()
                    response_code = response.status
                await asyncio.sleep(5)
                if response_code == 429:
                    print(f"{item} is rate limited")
                    await asyncio.sleep(
                        random.uniform(10.00, 25.00)
                        + random.uniform(-1.00, 1.00)
                        + random.uniform(-1.00, 1.00)
                    )
                    print("Finished sleeping")
                elif response_code == 200:
                    print(proxy)
                    autobuy_price = response_text["buy_order_graph"][0][0]
                    print(autobuy_price)
                    await update_autobuy_price(connection, item_id, autobuy_price)
        except Exception as e:
            print(f"Error making request to {item}: {e}")
            await asyncio.sleep(3)
            continue


async def gather_tasks(steam_links, num_parts, proxies, steam_cookie=None, items=None):
    tasks = []
    print(len(steam_links))
    if items:
        print(len(items))
    if items:
        sorted_items = sorted(items, key=lambda x: x['name'])
        sorted_links = sorted(steam_links, key=lambda x: x['name'])
        for item, link in zip(sorted_items, sorted_links):
            item['steam_link'] = link['steam_link']
        print(items)
        parts = numpy.array_split(items, num_parts)
    else:
        parts = numpy.array_split(steam_links, num_parts)

    for proxy, part, cookie in zip(
        proxies, parts, steam_cookie if steam_cookie else []
    ):
        print(proxy)
        print(len(part))
        tasks.append(asyncio.create_task(run_period(proxy, part, cookie)))
    print(tasks)
    await asyncio.gather(*tasks)


async def main():
    # Get proxies and items from database
    connection = await get_async_connection()
    proxies = await get_bot_credentials(connection, credential_type="proxy")
    outdated_steam_cookies = await get_bot_credentials(connection, credential_type='outdated_steam_cookie')
    outdated_steam_cookies = [c['id'] for c in outdated_steam_cookies]
    print(outdated_steam_cookies)
    
    steam_links = await get_item_data(connection, "non_id_links")
    for proxy in proxies:
        if proxy['id'] in outdated_steam_cookies:
            cookies = get_steam_cookie(
                proxy["proxy"], proxy["proxy_user"], proxy["proxy_password"]
            )
            await update_bot_credentials(
                connection,
                credential_type="steam_cookie",
                data=cookies,
                identifier=proxy["proxy"],
            )
        else:
            print('Steam cookie is up-to-date')
    print("Steam links:", steam_links)

    # Split items between proxies and process them asynchronously
    num_parts = len(proxies)

    if steam_links:
        await gather_tasks(
            steam_links = [link["steam_link"] for link in steam_links], 
            num_parts=num_parts, 
            proxies=proxies
        )
        print("Finished processing steam links.")
    steam_links = await get_item_data(connection, "links")
    await asyncio.sleep(3)
    steam_cookies = await get_bot_credentials(
        connection, credential_type="steam_cookie"
    )
    num_parts = len(steam_cookies)

    steam_ids = await get_item_data(connection, "steam_ids")
    print("Steam IDs:", steam_ids)

    await gather_tasks(steam_links, num_parts, proxies, steam_cookies, steam_ids)
    print("Finished processing steam IDs.")


asyncio.run(main())
