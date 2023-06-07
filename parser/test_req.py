import requests
import time
http = 'https://steamcommunity.com/market/itemordershistogram?country=RU&language=english&currency=15&item_nameid=176321160&two_factor=0'
proxy = 'http://S82GaU:WPe3gFmXfB@92.119.193.124:3000'
while True:
    response = requests.get(http, proxies={'http': proxy, 'https': proxy})
    print(response.json())
    time.sleep(5)
