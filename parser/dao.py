import psycopg2
from datetime import datetime, timedelta
import json
import time
import signal
import asyncpg
from utilities import try_decorator
# TODO
# Объединить GET, сократив кол-во запросов к БД
# Объединить запросы к таблицам в DAO объекты / репозитории

######################################## DB FUNCTIONS ##########################################

@try_decorator
async def get_async_connection():
    print("Connecting to db asynchronically...")
    conn = await asyncpg.connect(
        database="arbitrage",
        user="remote",
        password="1234qwer",
        host="89.108.114.215",
        port=5432,
        timeout=10,
    )
    print("Connection received")
    return conn

@try_decorator
def get_connection():
    """This function returns a sync connection to a PostgresSQL database."""

    conn = psycopg2.connect(
        "dbname='arbitrage' user='remote' password='1234qwer' host='89.108.114.215,5432'"
    )
    return conn

@try_decorator
def get_cursor(conn):
    """ This function creates a cursor object from the connection object passed as an argument."""
    # TODO Remove 
    cursor = conn.cursor()
    return cursor


############################################ BOTS TABLE ##############################################

@try_decorator
async def get_bot_credentials(conn, credential_type=None):
    """This function retrieves bot credentials from the database using a cursor."""

    if credential_type == "proxy":
        query = "SELECT id, proxy, proxy_user, proxy_password FROM bots ORDER BY id ASC"
        credential_keys = ["id", "proxy", "proxy_user", "proxy_password"]
        credential_filter = lambda cred: True
    elif credential_type == "buff_cookie":
        query = "SELECT id, buff_cookie, cookies_updated_at FROM bots"
        credential_keys = ["id", "cookie", "updated_at"]
        credential_filter = lambda cred: cred.get("cookie")
    elif credential_type == "steam_cookie":
        query = "SELECT id, steam_cookie, steam_cookie_updated_at FROM bots WHERE steam_cookie_updated_at > NOW() - INTERVAL '1 day'"
        credential_keys = ["id", "cookie", "updated_at"]
        credential_filter = lambda cred: cred.get("cookie")
    elif credential_type == "email":
        query = "SELECT id, email, email_password FROM bots ORDER BY id ASC"
        credential_keys = ["id", "email", "email_password"]
        credential_filter = lambda cred: cred.get("email") and cred.get("email_password")
    elif credential_type == "steam":
        query = "SELECT id, steam_login, steam_password FROM bots ORDER BY id ASC"
        credential_keys = ["id", "steam_login", "steam_password"]
        credential_filter = lambda cred: cred.get("steam_login") and cred.get("steam_password")
    elif credential_type == 'outdated_steam_cookie':
        query = "SELECT id FROM bots WHERE steam_cookie_updated_at < NOW() - INTERVAL '1 day'"
        credential_keys = ["id"]
        credential_filter = lambda cred: True
    else:
        raise ValueError(f"Invalid credential type: {credential_type}")
    
    rows = await conn.fetch(query)
    creds = [{key: row[i] for i, key in enumerate(credential_keys)} for row in rows if credential_filter({key: row[i] for i, key in enumerate(credential_keys)})]
    return creds

@try_decorator
async def update_bot_credentials(conn, credential_type, data, identifier):
    """This function inserts bot credentials into the database using a cursor."""
    if credential_type == "buff_cookie":
        print("Updating buff cookies")
        cookies = json.dumps(data)
        query = """UPDATE bots SET cookie = $1, cookies_updated_at = $2 WHERE id = $3"""

    elif credential_type == "steam_cookie":
        print('Updating steam cookies')
        cookies = json.dumps(data)
        print(cookies)
        query = """UPDATE bots SET steam_cookie = $1, steam_cookie_updated_at = $2 WHERE proxy = $3"""
    else:
        raise ValueError(f"Invalid credential type: {credential_type}")
    await conn.execute(query, cookies, datetime.now(), identifier)
    return True


################################################ ITEMS TABLE ##################################################

@try_decorator
async def get_item_data(conn, requested_data):
    if requested_data == 'steam_ids':
        query = "SELECT name, steam_id FROM items WHERE steam_id IS NOT NULL"
        credential_keys = ["name", "steam_id"]
    elif requested_data == 'buff_links':
        query = "SELECT name, link FROM items WHERE steam_id IS NOT NULL"
        credential_keys = ["name", "link"]
    elif requested_data == 'links':
        query = "SELECT name, steam_link FROM items WHERE steam_id IS NOT NULL"
        credential_keys = ["name", "steam_link"]
    elif requested_data == 'non_id_links':
        query = "SELECT name, steam_link FROM items WHERE steam_id IS NULL"
        credential_keys = ["name", "steam_link"]
    elif requested_data == 'item_ids':
        query = "SELECT name, item_id FROM items"
        credential_keys = ["name", "item_id"]
    else:
        raise ValueError(f"Invalid requested data: {requested_data}")
    
    data = [{key: row[i] for i, key in enumerate(credential_keys)} for row in await conn.fetch(query)]
    print(data)
    return data

@try_decorator
async def insert_items(pool, items):
    async with pool.acquire() as conn:
        while True:
            try:
                async with conn.transaction():
                    await conn.execute("SET LOCAL statement_timeout = '7s'")
                    placeholders = ", ".join(["%s"] * len(items[0]))
                    query = f"""INSERT INTO items (item_id, link, lowest_price, steam_price, name, updated_at, steam_link)
                                VALUES ({placeholders})
                                ON CONFLICT (item_id) DO UPDATE SET lowest_price = EXCLUDED.lowest_price, updated_at = EXCLUDED.updated_at"""
                    values = [
                        [str(value).replace("'", "''") for value in item.values()]
                        for item in items
                    ]
                    await conn.executemany(query, values)
                    break
            except asyncpg.exceptions.PostgresError as e:
                if "timeout" in str(e):
                    print(f"Error: {e}")
                else:
                    raise e
                

@try_decorator
async def insert_steam_ids(conn, steam_ids):
    try:
        async with conn.transaction():
            await conn.execute("SET SESSION statement_timeout = 7000")
            for link, id in steam_ids.items():
                await conn.execute(
                    """
                    UPDATE items SET steam_id = $1 WHERE steam_link = $2
                    """,
                    id,
                    link,
                )
            return
    except asyncpg.exceptions.PostgresError as e:
        if "timeout" in str(e):
            print(f"Error: {e}")
        else:
            raise e


@try_decorator
async def update_autobuy_price(conn, steam_id, price):
    try:
        async with conn.transaction():
            await conn.execute("SET SESSION statement_timeout = 7000")
            await conn.execute(
                """
                UPDATE items SET autobuy_price = $1 WHERE steam_id = $2
                """,
                price,
                steam_id,
            )
            print(f'Inserted autobuy price for item {steam_id}')
            return
    except asyncpg.exceptions.PostgresError as e:
        if "timeout" in str(e):
            print(f"Error: {e}")
        else:
            raise e

@try_decorator
async def update_prices(
    conn, item_id, lowest_price, second_price, last_sell_price, autobuy_price
):
    while True:
        async with conn.transaction():
            try:
                await conn.execute("SET SESSION statement_timeout = 7000")
                await conn.execute(
                    "UPDATE items SET lowest_price = $1, second_price = $2, last_sell_price = $3, price_updated_at = $4, autobuy_price = $5 WHERE item_id = $6",
                    lowest_price,
                    second_price,
                    last_sell_price,
                    datetime.now(),
                    None,
                    item_id,
                )
                break
            except Exception as e:
                print(e)
                await conn.execute("ROLLBACK")

@try_decorator
async def update_category(conn, item_id, item_category):
    while True:
        async with conn.transaction():
            try:
                await conn.execute("SET SESSION statement_timeout = 7000")
                await conn.execute(
                    "UPDATE items SET categories = $1 WHERE item_id = $2",
                    item_category,
                    item_id,
                )
                break
            except Exception as e:
                print(e)
                await conn.execute("ROLLBACK")




################################################################################################################
########################################### ONE TIME FUNCTIONS #################################################

@try_decorator
def insert_bots(conn, cursor):
    with open("bots.txt", "r") as f:
        for line in f:
            row = line.strip().split("\t")
            steam_login, steam_password, email, email_password, proxy = row
            print(proxy, steam_login, steam_password, email, email_password)
            query = "INSERT INTO bots (proxy, steam_login, steam_password, email, email_password) VALUES (%s, %s, %s, %s, %s)"
            values = (proxy, steam_login, steam_password, email, email_password)
            cursor.execute(query, values)
        conn.commit()
