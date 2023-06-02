import psycopg2
from datetime import datetime
import json

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

def get_buff_credentials(cursor):
    bots = []
    cursor.execute(
        "SELECT id, proxy, cookie, proxy_user, proxy_password FROM bots ORDER BY id ASC"
    )
    rows = cursor.fetchall()

    for row in rows:
        raw_cookie = row[2]
        key_value_pairs = raw_cookie.split("; ")
        cookies = {}
        for pair in key_value_pairs:
            key, value = pair.split("=")
            cookies[key] = value
        print(cookies)
        bot_credentials = {
            "id": row[0],
            "proxy": row[1],
            "cookie": cookies,
            "proxy_user": row[3],
            "proxy_password": row[4],
        }

        bots.append(bot_credentials)

    return bots

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
    for item in items:
        item_values = ", ".join(
            [f"""'{str(value).replace("'", "''")}'""" for value in item.values()]
        )
        cursor.execute(
            f"""INSERT INTO items (item_id, link, site_price, steam_price, name, updated_at, steam_link)
            VALUES ({item_values})
            ON CONFLICT (item_id) DO UPDATE SET site_price = '{item['site_price']}', updated_at = '{datetime.now()}'
            """
        )
    conn.commit()

async def insert_cookies(cursor, bot_id, cookies, conn):
    cookies = json.dumps(cookies)
    print(cookies)
    cursor.execute(
        """
        UPDATE bots SET cookie = %s WHERE id = %s
        """, (cookies, bot_id)
    )
    conn.commit()