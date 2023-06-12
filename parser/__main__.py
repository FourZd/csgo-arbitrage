import time
import math
import asyncio
import random
import datetime
import aiohttp
import json

from dbo import (
    get_cookies,
    get_cursor,
    get_connection,
    get_proxy_creds,
    get_steam_credentials,
    get_email_credentials,
    insert_items,
    insert_cookies,
    get_item_ids,
    update_category,
    update_prices,
    get_async_connection
)
from run_selenium import login, get_firefox_options


async def run_chunk(proxy, cookie, chunk, category: bool = False, cookie_timeout: datetime = None):
    while time.now() < cookie_timeout:
        async with aiohttp.ClientSession() as session:
            cookie = json.loads(cookie)
            count = 0
            print(cookie)
            try:
                session.cookie_jar.update_cookies(cookie)
            except Exception as e:
                print("Error", e)
                print(cookie)
            print(f'Chunk {chunk} has started at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            conn = await get_async_connection()
            for item_id in chunk:
                print(f'Parsed {count} pages from total {len(chunk)} pages')
                print('Item id', item_id)
                while True:
                    try:
                        async with session.get(
                                f'https://buff.163.com/api/market/goods/sell_order?game=csgo&goods_id={item_id}&page_num=1&sort_by=default&mode=&allow_tradable_cooldown=1&_={round(time.time() * 1000)}',
                                proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_host']}",
                                timeout=10,
                        ) as response:
                            try:
                                item = await response.json()
                            except Exception as e:
                                print(e)
                        try:
                            if item['code'] == "OK":
                                try:
                                    item_category = item['data']['goods_infos'][item_id]["tags"]["rarity"]["localized_name"]
                                    lowest_price = item['data']['items'][0]['price']
                                except (IndexError, KeyError):
                                    print('No items')
                                    lowest_price = None
                                    item_category = None
                                try:
                                    second_price = item['data']['items'][1]['price']
                                except (IndexError, KeyError):
                                    print('No second item')
                                    second_price = None

                                async with session.get(
                                    f'https://buff.163.com/api/market/goods/bill_order?game=csgo&goods_id={item_id}&_={round(time.time() * 1000)}',
                                    proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_host']}",
                                    timeout=10,
                                ) as response:
                                    try:
                                        sell_history = await response.json()
                                    except Exception as e:
                                        print(e)
                                last_sell_price = sell_history['data']['items'][0]['price']
                                if category == True:
                                    await update_category(conn, item_id, item_category)
                                
                                await update_prices(conn, item_id, lowest_price, second_price, last_sell_price)
                                count += 1
                            break
                        except Exception as e:
                            print(e)
                    except asyncio.TimeoutError:
                        print('TIMEOUT ERROR')
                    except aiohttp.client_exceptions.ClientConnectorSSLError as e:
                        print('SSL ERROR, fix later')
                print('Left the while cycle', item_id)
                print('Sleeping...', item_id)
                await asyncio.sleep(random.uniform(3.0, 4.0) + random.uniform(-1.0, 1.0))
                print("Finished sleeping!")
    raise TimeoutError

async def split_list(lst, chunk_size):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]

async def run_parser(cursor, conn, proxy_creds, cookies_list):
    async with aiohttp.ClientSession() as session:
        # Get amount of bots to split pages between them
        bots_amount = len(cookies_list)
        links = get_item_ids(cursor)
        period_size = len(links) // bots_amount
        chunks = await split_list(links, period_size)
        
        tasks = []
        for cookie, chunk in zip(cookies_list, chunks):
            # TODO create get_proxy_by_id
            proxy = [
            {
                "proxy_host": proxy["proxy"],
                "proxy_username": proxy["proxy_user"],
                "proxy_password": proxy["proxy_password"],
            }
            for proxy in proxy_creds
            if proxy["id"] == cookie["id"]
            ][0]
            cookie_timeout = cookie["updated_at"] + datetime.timedelta(days=1)
            tasks.append(
                asyncio.create_task(
                    run_chunk(proxy, cookie['cookie'], chunk, False, cookie_timeout)
                )
            )
        print('Starting parser...')
        await asyncio.gather(*tasks)


def generate_cookies(conn, cursor, steam_creds, proxy_creds, email_creds):
    print('Generating cookies...')
    for steam in steam_creds:

        # TODO create get_proxy_by_id
        proxy = [
            {
                "proxy_host": proxy["proxy"],
                "proxy_username": proxy["proxy_user"],
                "proxy_password": proxy["proxy_password"],
            }
            for proxy in proxy_creds
            if proxy["id"] == steam["id"]
        ]
        # TODO create get_email_by_id
        email = [
            {
                "email": email["email"],
                "email_password": email["email_password"]
            }
            for email in email_creds
            if email["id"] == steam["id"]
        ]
        if len(proxy) > 0 and len(email) > 0:
            proxy = proxy[0]
            email = email[0]
            options = get_firefox_options(
                proxy["proxy_host"], proxy["proxy_username"], proxy["proxy_password"]
            )
            cookies = login(
                steam["steam_login"],
                steam["steam_password"],
                email["email"],
                email["email_password"],
                options,
            )
            insert_cookies(cursor, steam["id"], cookies, conn)

def get_cookies_to_update(steam_creds, cookies_list):
    print('Getting cookies to update...')
    cookies_to_update = []
    for cred in steam_creds:
        if cred['id'] not in [cookie['id'] for cookie in cookies_list]:
            cookies_to_update.append(cred)
        else:
            for cookie in cookies_list:
                if cookie['id'] == cred['id']:
                    if cookie['updated_at'] < (datetime.datetime.now() - datetime.timedelta(days=1)):
                        cookies_to_update.append(cred)
    return cookies_to_update

async def main():
    conn = get_connection()
    cursor = get_cursor(conn)

    # Get steam/mail credentials for authentication with proxy
    steam_creds = get_steam_credentials(cursor)
    proxy_creds = get_proxy_creds(cursor)
    email_creds = get_email_credentials(cursor)
    cookies_list = get_cookies(cursor)
    cookies_to_update = get_cookies_to_update(steam_creds, cookies_list)
    
    generate_cookies(conn, cursor, cookies_to_update, proxy_creds, email_creds)
    cookies_list = get_cookies(cursor)
    while True:
        try:
            await run_parser(cursor, conn, proxy_creds, cookies_list)
        except TimeoutError as e:
            print('Cookie expired')

            cookies_to_update = get_cookies_to_update(steam_creds, cookies_list)
            generate_cookies(conn, cursor, cookies_to_update, proxy_creds, email_creds)
            cookies_list = get_cookies(cursor)


if __name__ == "__main__":
    asyncio.run(main())
