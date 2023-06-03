import requests
import time
import math
import asyncio
import random
from datetime import datetime
import re

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
    print(f'Period {period} has started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    for i in range(period[0], period[1] + 1):
        page = await get_page(
            page_num=i,
            cookies=bot["cookie"],
            game=game,
            proxy=bot["proxy"],
            proxy_username=bot["proxy_user"],
            proxy_password=bot["proxy_password"],
        )
        try:
            item_list = []
            items = page["data"]["items"]
            for item in items:
                skin = {
                    "item_id": item["id"],
                    "link": f"https://buff.163.com/goods/{item['id']}#tab=selling",
                    "site_price": item["sell_min_price"],
                    "steam_price": item["goods_info"]["steam_price_cny"],
                    "name": item["market_hash_name"],
                    "updated_at": datetime.now(),
                    "steam_link": item["steam_market_url"],
                }
                item_list.append(skin)
            print(f'Item list contains {len(item_list)} items')
            await insert_items(cursor, conn, item_list)
        except KeyError:
            print('WARNING')
            print('KEY EXCEPTION ON PAGE', i)
            print('USING BOT', bot)


async def get_page(
    page_num, cookies, game, proxy, proxy_username, proxy_password, rarity=None
):
    url = (
        
    )
    url = (
        f"https://buff.163.com/api/market/goods?game={game}&page_num={page_num}&use_suggestion=0&_={round(time.time() * 1000)}"
        + (f"&rarity={rarity}" if rarity else "")
    )
    print(f'Requesting page {url}')
    response = requests.get(
        url,
        cookies=cookies,
        proxies={
            "http": f"http://{proxy_username}:{proxy_password}@{proxy}",
            "https": f"https://{proxy_username}:{proxy_password}@{proxy}",
        },
        timeout=50,
    )
    print('Status code is', response.status_code)
    print('Sleeping... Zzz....')
    await asyncio.sleep(random.uniform(2.00, 5.00))
    print("Sleep done", page_num)
    return response.json()


async def run_parser(cursor, conn, bots):
    # Get amount of bots to split pages between them
    bots_amount = len(bots)

    # We will use the first bot to get the amount of pages
    first_bot = bots[0]

    # Get total page number
    total_pages = await get_page(
        page_num=1,
        cookies=first_bot["cookie"],
        game="csgo",
        proxy=first_bot["proxy"],
        proxy_username=first_bot["proxy_user"],
        proxy_password=first_bot["proxy_password"],
    )
    try:
        total_pages = total_pages["data"]["total_page"]
    except KeyError:
        print(total_pages)

    # Split pages between bots
    period_size = math.ceil(total_pages / bots_amount)
    print("Period size is", period_size)

    tasks = []
    for i in range(bots_amount):
        # [(1, 15), (16, 30), (31, 45), (46, 60)]
        if i == 0:
            period = (i + 1, period_size)
        else:
            period = (i * period_size + 1, (i + 1) * period_size)
        tasks.append(
            asyncio.create_task(
                run_period(
                    bot=bots[i], period=period, game="csgo", conn=conn, cursor=cursor
                )
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
        print(proxy)
        if len(proxy) > 0:
            print(proxy)
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


async def run():
    conn = get_connection()
    cursor = get_cursor(conn)

    # Get bots credentials for authentication with proxy
    bots = get_buff_credentials(cursor, cookie=False)
    await get_cookies(cursor, bots, conn)

    bots = get_buff_credentials(cursor, cookie=True)
    await run_parser(cursor, conn, bots)


if __name__ == "__main__":
    asyncio.run(run())
