import psycopg2
from datetime import datetime, timedelta
import json
import time
import signal
import asyncpg

def get_connection():
    conn = psycopg2.connect(
        "dbname='arbitrage' user='remote' password='1234qwer' host='89.108.114.215,5432'"
    )
    return conn


def get_cursor(conn):
    cursor = conn.cursor()
    return cursor


def get_items(cursor, **requested_fields):
    cursor.execute(f"SELECT {','.join(requested_fields.keys())}")

def get_buff_credentials(cursor, cookie: bool):
    bots = []
    cursor.execute(
        "SELECT id, proxy, cookie, proxy_user, proxy_password, cookies_updated_at FROM bots ORDER BY id ASC"
    )
    rows = cursor.fetchall()

    for row in rows:
        cookies = None
        raw_cookie = row[2]
        if raw_cookie:
            cookies = json.loads(raw_cookie)
        bot_credentials = {
            "id": row[0],
            "proxy": row[1],
            "cookie": cookies,
            "proxy_user": row[3],
            "proxy_password": row[4],
            "cookies_updated_at": row[5],
        }
        if cookie and bot_credentials.get("cookie"):
            bots.append(bot_credentials)
        elif not cookie and (
            not bot_credentials.get("cookie")
            or not bot_credentials.get("cookies_updated_at")
            or not bot_credentials.get("cookies_updated_at")
            > datetime.now() - timedelta(days=1)
        ):
            bots.append(bot_credentials)

    return bots

def get_steam_ids(cursor):
    cursor.execute(
        "SELECT steam_id FROM items WHERE steam_id IS NOT NULL"
    )
    rows = cursor.fetchall()
    steam_ids = []
    for row in rows:
        steam_ids.append(row[0])
    return steam_ids
def get_steam_credentials(cursor):
    creds = []
    cursor.execute(
        "SELECT id, steam_login, steam_password, email, email_password FROM bots ORDER BY id ASC"
    )
    rows = cursor.fetchall()

    for row in rows:
        creds.append(
            {
                "id": row[0],
                "steam_login": row[1],
                "steam_password": row[2],
                "email": row[3],
                "email_password": row[4],
            }
        )

    return creds


async def insert_items(cursor, conn, items):
    print(len(items))
    while True:
        try:
            with conn.cursor() as cur:
                # Set statement_timeout only for this transaction
                cur.execute("SET LOCAL statement_timeout = '7s'")

                # Begin transaction explicitly
                cur.execute("BEGIN")

                # Prepare the parameterized query
                placeholders = ", ".join(["%s"] * len(items[0]))
                query = f"""INSERT INTO items (item_id, link, site_price, steam_price, name, updated_at, steam_link)
                            VALUES ({placeholders})
                            ON CONFLICT (item_id) DO UPDATE SET site_price = EXCLUDED.site_price, updated_at = EXCLUDED.updated_at
                            """

                # Execute the query for all items
                values = [
                    [str(value).replace("'", "''") for value in item.values()]
                    for item in items
                ]
                cur.executemany(query, values)

                # Commit transaction explicitly
                print("Committing changes")
                conn.commit()
                print("Committed")
        except psycopg2.errors.QueryCanceledError as e:
            print(f"Error: {e}")
            conn.rollback()
            print("Rolled back changes")
        except psycopg2.errors.InFailedSqlTransaction as e:
            print(f"Error: {e}")
            conn.rollback()
            print("Rolled back changes")


async def insert_cookies(cursor, bot_id, cookies, conn):
    cookies = json.dumps(cookies)
    print(cookies)
    cursor.execute(
        """
        UPDATE bots SET cookie = %s, cookies_updated_at = %s WHERE id = %s
        """,
        (cookies, datetime.now(), bot_id),
    )
    conn.commit()


def get_steam_links(cursor):
    cursor.execute("SELECT steam_link FROM items WHERE steam_id IS NULL")
    rows = [row[0] for row in cursor.fetchall()]
    return rows

def raise_timeout(signum, frame):
    raise TimeoutError("Timeout")

async def get_async_connection():
    conn = await asyncpg.connect(
        database='arbitrage',
        user='remote',
        password='1234qwer',
        host='89.108.114.215',
        port=5432,
        timeout=10,
    )
    return conn
async def insert_steam_ids(conn, steam_ids):
    print('Timeout set')
    while True:
        try:
            async with conn.transaction():
                # Set the statement_timeout parameter to 7 seconds
                await conn.execute("SET SESSION statement_timeout = 7000")
                for link, id in steam_ids.items():
                    await conn.execute(
                        """
                        UPDATE items SET steam_id = $1 WHERE steam_link = $2
                        """,
                        id, link
                    )
                print("Committed")
                break
        except asyncpg.exceptions.PostgresError as e:
            if "timeout" in str(e):
                print(f"Error: {e}")
                await conn.execute('ROLLBACK')
                print("Rolled back changes")
            else:
                raise e


def get_proxies(cursor):
    cursor.execute("SELECT proxy_host, proxy_username, proxy_password FROM proxies")
    rows = cursor.fetchall()
    proxies = []
    for row in rows:
        proxies.append(
            {
                "proxy": row[0],
                "proxy_username": row[1],
                "proxy_password": row[2],
            }
        )
    return proxies
def insert_proxies(cursor, conn):
    # Open the proxies.txt file and read its contents
    with open('proxies.txt', 'r') as file:
        proxies = file.readlines()

    # Loop through each line in the file and parse the proxy information
    for proxy in proxies:
        parts = proxy.strip().split('@')
        host_port = parts[0]
        username_password = parts[1]

        # Parse the username and password from the second part of the string
        username, password = username_password.split(':')

        print('inserting proxy')
        # Insert the proxy into the database
        print(host_port, username, password)
        query = f"""INSERT INTO proxies (proxy_host, proxy_username, proxy_password)
                    VALUES (%s, %s, %s);
                """

        cursor.execute(query,
                       (host_port, username, password))
        print('inserted proxy')
    # Commit the changes to the database
    conn.commit()


async def insert_steam_price(conn, steam_id, price):
    while True:
        try:
            async with conn.transaction():
                # Set the statement_timeout parameter to 7 seconds
                await conn.execute("SET SESSION statement_timeout = 7000")
                await conn.execute(
                    """
                    UPDATE items SET steam_price = $1 WHERE steam_id = $2
                    """,
                    price, steam_id
                )
                print("Committed")
                break
        except asyncpg.exceptions.PostgresError as e:
            print(e)
            await conn.execute('ROLLBACK')

def insert_bots(conn, cursor):
    with open('bots.txt', 'r') as f:
        for line in f:
            row = line.strip().split('\t')
            steam_login, steam_password, email, email_password, proxy = row
            print(proxy, steam_login, steam_password, email, email_password)
            query = "INSERT INTO bots (proxy, steam_login, steam_password, email, email_password) VALUES (%s, %s, %s, %s, %s)"
            values = (proxy, steam_login, steam_password, email, email_password)
            cursor.execute(query, values)
        conn.commit()
