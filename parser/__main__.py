import time
import math
import asyncio
import random
from datetime import datetime

import aiohttp

from dbo import (
    get_cursor,
    get_connection,
    get_buff_credentials,
    get_steam_credentials,
    insert_items,
    insert_cookies,
)
from run_selenium import login, get_firefox_profile


async def run_period(cursor, conn, bot, period, game):
    async with aiohttp.ClientSession() as session:
        
        count = 0
        session.cookie_jar.update_cookies(bot['cookie'])
        print(f'Period {period} has started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        for i in range(period[0], period[1] + 1):
            print(f'Parsed {count} pages from total {period[1]-period[0]+1} pages')
            print('Page', i)
            while True:
                try:
                    async with session.get(
                            f"https://buff.163.com/api/market/goods?game={game}&page_num={i}&use_suggestion=0&_={round(time.time() * 1000)}",
                            cookies=bot["cookie"],
                            proxy=f"http://{bot['proxy_user']}:{bot['proxy_password']}@{bot['proxy']}",
                            timeout=3,
                    ) as response:
                        page = await response.json()
                        try:
                            items = page["data"]["items"]
                            item_list = [
                                {
                                    "item_id": item["id"],
                                    "link": f"https://buff.163.com/goods/{item['id']}#tab=selling",
                                    "site_price": item["sell_min_price"],
                                    "steam_price": item["goods_info"]["steam_price_cny"],
                                    "name": item["market_hash_name"],
                                    "updated_at": datetime.now(),
                                    "steam_link": item["steam_market_url"],
                                }
                                for item in items
                            ]
                            print(f'Collected {len(item_list)} items from page {i}')
                            print('Inserted items into database from page', i)
                            await insert_items(cursor, conn, item_list)
                            count += 1
                            print('Inserted! Breaking the while', i)
                            break
                        except KeyError:
                            print('WARNING')
                            print('KEY EXCEPTION ON PAGE', i)
                            print('USING BOT', bot)
                except asyncio.TimeoutError:
                    print('TIMEOUT ERROR, PROBABLY BANNED')
                except aiohttp.client_exceptions.ClientConnectorSSLError as e:
                    print('SSL ERROR, fix later')
            print('Left the while', i)
            print('Sleeping...', i)
            await asyncio.sleep(random.uniform(3.0, 7.0) + random.uniform(-1.0, 1.0))
            print("Finished sleeping!")

async def run_parser(cursor, conn, bots):
    # Get amount of bots to split pages between them
    bots_amount = len(bots)

    # We will use the first bot to get the amount of pages
    first_bot = bots[0]
    async with aiohttp.ClientSession() as session:
        session.cookie_jar.update_cookies(first_bot["cookie"])
        async with session.get(
                f"https://buff.163.com/api/market/goods?game=csgo&page_num=1&use_suggestion=0&_={round(time.time() * 1000)}",
                cookies=first_bot["cookie"],
                proxy=f"http://{first_bot['proxy_user']}:{first_bot['proxy_password']}@{first_bot['proxy']}",
                timeout=5,
        ) as response:
            total_pages = await response.json()
        try:
            total_pages = total_pages["data"]["total_page"]
        except KeyError:
            print(total_pages)

        # Split pages between bots
        period_size = math.ceil(total_pages / bots_amount)
        print("Period size is", period_size)

        tasks = []
        for i, bot in enumerate(bots):
            # [(1, 15), (16, 30), (31, 45), (46, 60)]
            if i == 0:
                period = (i + 1, period_size)
            else:
                period = (i * period_size + 1, (i + 1) * period_size)
            tasks.append(
                asyncio.create_task(
                    run_period(cursor, conn, bot, period, "csgo")
                )
            )
        print('Starting parser...')
        await asyncio.gather(*tasks)


async def get_cookies(cursor, bots, conn):
    # Get bots credentials for authentication with proxy

    steam_creds = get_steam_credentials(cursor)
    for account in steam_creds:
        proxy = [
            {
                "proxy_host": bot["proxy"],
                "proxy_username": bot["proxy_user"],
                "proxy_password": bot["proxy_password"],
            }
            for bot in bots
            if bot["id"] == account["id"]
        ]
        if len(proxy) > 0:
            proxy = proxy[0]

            profile = await get_firefox_profile(
                proxy["proxy_host"], proxy["proxy_username"], proxy["proxy_password"]
            )
            cookies = await login(
                account["steam_login"],
                account["steam_password"],
                account["email"],
                account["email_password"],
                profile,
            )
            await insert_cookies(cursor, account["id"], cookies, conn)


async def main():
    conn = get_connection()
    cursor = get_cursor(conn)

    # Get bots credentials for authentication with proxy
    bots = get_buff_credentials(cursor, cookie=False)
    await get_cookies(cursor, bots, conn)

    bots = get_buff_credentials(cursor, cookie=True)
    await run_parser(cursor, conn, bots)


if __name__ == "__main__":
    asyncio.run(main())
