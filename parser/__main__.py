import requests
from time import sleep
game = 'csgo'
_ = 1685300799899
page_num = 0
rarity = 'common_weapon'
'Device-Id=afWgKoyOveCIwVEvF0wv; csrf_token=IjJmZjA5MzE4NTQxM2E1NjJmOWU3YzU4MTI0OThiYWVlYTU2MTkwYzki.F1U5bw.0rdRk9aTM6MPXiFOPtmlw4I7jR8; Locale-Supported=en; game=csgo; session=1-J2_puuVxT2LXJPlSqVRC40NgC_J6kTYphfoqNSP22EPQ2030612033'
def call(page_num):
    url = f'https://buff.163.com/api/market/goods?game={game}&page_num={page_num}&use_suggestion=0&_={_}&rarity={rarity}'
    r = requests.get(url, cookies={'Device-Id': 'afWgKoyOveCIwVEvF0wv', 'csrf_token': 'IjJmZjA5MzE4NTQxM2E1NjJmOWU3YzU4MTI0OThiYWVlYTU2MTkwYzki.F1U5bw.0rdRk9aTM6MPXiFOPtmlw4I7jR8', 'Locale-Supported': 'en', 'game': game,'session': '1-J2_puuVxT2LXJPlSqVRC40NgC_J6kTYphfoqNSP22EPQ2030612033'})
    return r.json()

total_pages = call(1)['data']['total_page']
sleep(5)
while page_num < total_pages:
    page_num += 1
    print('page', page_num)
    raw = call(page_num)
    
    items = raw['data']['items']

    with open('items.txt', 'a', encoding='utf-8') as f:
        for item in items:
            item_id = item['id']
            name = item['market_hash_name']
            buff_price_cny = item['sell_min_price']
            steam_price_cny = item['goods_info']['steam_price_cny']
            steam_url = item['steam_market_url']
            print(f'Writing {name} to items.txt')
            f.write(f'Name - {name}\nBuff price - {buff_price_cny}\nSteam price - {steam_price_cny}\nSteam url - {steam_url}\n=======================')
    print('sleeping...')
    sleep(3)
    print('sleep over')