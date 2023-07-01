import time
import math
import asyncio
import random
import datetime
import aiohttp
import json

from dao import (
    get_bot_credentials,
    get_cursor,
    get_connection,
    insert_items,
    update_bot_credentials,
    get_item_data,
    update_category,
    update_prices,
    get_async_connection
)
from run_selenium import login, get_firefox_options


async def run_chunk(proxy, chunk, category: bool = False):
    #while datetime.datetime.now() < cookie_timeout:
    print('Running chunk')
    while True:
        async with aiohttp.ClientSession() as session:
            print('Sleeping to random start time...')
            await asyncio.sleep(random.uniform(0.000, 40.500) + random.uniform(0.000, 120.000))
            print('Sleeping done!')
            # cookie = json.loads(cookie)
            count = 0
            # print(cookie)
            # try:
            #     session.cookie_jar.update_cookies(cookie)
            # except Exception as e:
            #     print("Error", e)
            #     print(cookie)
            print(f'Chunk {chunk} has started at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            conn = await get_async_connection()
            print('Got connected async')
            for item_id in chunk:
                item_id = item_id['item_id']
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
                                print('Getting json')
                                item = await response.json()
                                print('Json returned successfully')
                            except Exception as e:
                                print('Error getting json')
                                print(e)
                                print('SLeeping')
                                await asyncio.sleep(30)
                                print('Breaking')
                                break
                        print('Sleeping after request...')
                        await asyncio.sleep(random.uniform(4.00, 4.50) + random.uniform(-1.00, 1.00))
                        print('Sleeping done!')
                        # async with session.get(
                        #     f'	https://buff.163.com/api/market/goods/buy_order?game=csgo&goods_id={item_id}&page_num=1&_={round(time.time() * 1000)}',
                        #     proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_host']}",
                        #     timeout=10
                        # ) as response:
                        #     try:
                        #         print('Getting json')
                        #         autobuy = await response.json()
                        #         print('Json returned successfully')
                        #     except Exception as e:
                        #         print(e)
                        try:
                            if item['code'] == "OK": #and autobuy['code'] == "OK":
                            #     try:
                            #         autobuy_price = autobuy['data']['items'][0]['price']
                            #     except (IndexError, KeyError):
                            #         autobuy_price = 0
                            #         print('No autobuys')

                                try:
                                    print('Trying to get lowest price and category')
                                    item_category = item['data']['goods_infos'][item_id]["tags"]["rarity"]["localized_name"]
                                    lowest_price = item['data']['items'][0]['price']
                                    print('Successfully got lowest price and category')
                                except (IndexError, KeyError):
                                    print('No items')
                                    lowest_price = None
                                    item_category = None
                                try:
                                    print('Getting second price')
                                    second_price = item['data']['items'][1]['price']
                                    print('Second price successfuly received')
                                except (IndexError, KeyError):
                                    print('No second item')
                                    second_price = None
                                
                                # async with session.get(
                                #     f'https://buff.163.com/api/market/goods/bill_order?game=csgo&goods_id={item_id}&_={round(time.time() * 1000)}',
                                #     proxy=f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_host']}",
                                #     timeout=10,
                                # ) as response:
                                #     try:
                                #         sell_history = await response.json()
                                #     except Exception as e:
                                #         print(e)
                                # last_sell_price = sell_history['data']['items'][0]['price']
                                if category == True:
                                    print('Updating item category')
                                    await update_category(conn, item_id, item_category)
                                    print('Category updated')
                                print('Updating price')
                                await update_prices(conn, item_id, lowest_price, second_price, last_sell_price=None, autobuy_price=None)
                                print('Price updated')
                                count += 1
                                print('Count +1')
                                print('Breaking cycle')
                                break
                            else:
                                print(item['code'])
                                print('ERROR 0')
                        except Exception as e:
                            print(e)
                            print('ERROR 1')
                            await asyncio.sleep(10)
                    except asyncio.TimeoutError:
                        print('TIMEOUT ERROR')
                    except aiohttp.client_exceptions.ClientConnectorSSLError as e:
                        print('SSL ERROR, fix later')
                    except Exception as e:
                        print('ERROR 2')
                        await asyncio.sleep(random.uniform(60.0, 120.0))
                        print(e)
                
                print('Left the while cycle', item_id)
                print('Sleeping...', item_id)
                await asyncio.sleep(random.uniform(2.0, 4.0) + random.uniform(-1.0, 1.0))
                print("Finished sleeping!")
        raise TimeoutError

async def split_list(lst, chunk_size):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]

async def run_parser(conn, proxy_creds, cookies_list):
    async with aiohttp.ClientSession() as session:
        # Get amount of bots to split pages between them
        bots_amount = len(proxy_creds)
        ids = await get_item_data(conn, 'item_ids')
        period_size = len(ids) // bots_amount
        chunks = await split_list(ids, period_size)
        
        tasks = []
        for proxy, chunk in zip(proxy_creds, chunks):
            # TODO create get_proxy_by_id
            proxy = {
                "proxy_host": proxy["proxy"],
                "proxy_username": proxy["proxy_user"],
                "proxy_password": proxy["proxy_password"],
            }
            #cookie_timeout = cookie["updated_at"] + datetime.timedelta(days=1)
            tasks.append(
                asyncio.create_task(
                    run_chunk(proxy, chunk, False)
                )
            )
        print('Starting parser...')
        await asyncio.gather(*tasks)


async def generate_cookies(conn, cursor, steam_creds, proxy_creds, email_creds):
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
            await update_bot_credentials(cursor, steam["id"], cookies, conn)

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
    async_conn = await get_async_connection()
    # Get steam/mail credentials for authentication with proxy
    steam_creds = await get_bot_credentials(async_conn, 'steam')
    proxy_creds = await get_bot_credentials(async_conn, 'proxy')
    email_creds = await get_bot_credentials(async_conn, 'email')

    ########### Buff cookies are currently not working due to bans
    cookies_list = None
    # cookies_list = await get_bot_credentials(async_conn, 'buff_cookie')
    # cookies_to_update = get_cookies_to_update(steam_creds, cookies_list)
    # await generate_cookies(conn, cursor, cookies_to_update, proxy_creds, email_creds)
    # cookies_list = await get_bot_credentials(async_conn, 'buff_cookie')
    while True:
        #try:
        await run_parser(async_conn, proxy_creds, cookies_list)
        # except TimeoutError as e:
        #     print('Cookie expired')

        #     cookies_to_update = get_cookies_to_update(steam_creds, cookies_list)
        #     await generate_cookies(conn, cursor, cookies_to_update, proxy_creds, email_creds)
        #     cookies_list = await get_bot_credentials(async_conn, 'buff_cookie')


if __name__ == "__main__":
    asyncio.run(main())
