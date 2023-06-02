from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio
import imaplib
import re

async def get_verification_code(email, email_password):
    mail = imaplib.IMAP4_SSL("imap.rambler.ru", 993)

    mail.login(email, email_password)
    mail.select("inbox")

    search_criteria = 'FROM "Steam Support" SUBJECT "Your Steam account: Access from new web or mobile device"'
    result, data = mail.search(None, search_criteria)
    latest_email = data[0].split()[-1]
    result, data = mail.fetch(latest_email, "(RFC822)")
    raw_email = data[0][1]
    raw_email_string = raw_email.decode("utf-8")
    code = re.findall(r"Login Code\s+(\w+)", raw_email_string)[0]
    return code


async def get_firefox_profile(proxy, proxy_username, proxy_password):
    proxy_host = re.findall(r"^(.*?):", proxy)[0]
    proxy_port = re.findall(r":\s*(.*)", proxy)[0]
    print(proxy_host)
    print(proxy_port)
    profile = webdriver.FirefoxProfile()
    profile.set_preference('network.proxy.type', 1)
    profile.set_preference('network.proxy.http', proxy_host)
    profile.set_preference('network.proxy.http_port', proxy_port)
    profile.set_preference('network.proxy.ssl', proxy_host)
    profile.set_preference('network.proxy.ssl_port', proxy_port)
    profile.set_preference('network.proxy.socks_username', proxy_username)
    profile.set_preference('network.proxy.socks_password', proxy_password)

    return profile

async def login(steam_login, steam_password, email, email_password, profile):
    
    driver = webdriver.Firefox(firefox_profile=profile)
    
    driver.get("https://buff.163.com/")
    driver.find_element(By.XPATH, "/html/body/div[1]/div/div[3]/ul/li/a").click()
    WebDriverWait(driver, 3)
    while True:
        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "remember-me"))
            ).click()
            break
        except Exception as e:
            print(e)
            WebDriverWait(driver, 3)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "j_login_other"))
    ).click()

    buff_window = driver.current_window_handle
    for window in driver.window_handles:
        if window != buff_window:
            driver.switch_to.window(window)
            break
    steam_login_window = driver.current_window_handle
    # Wait for username and password fields to be clickable
    username_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[1]/input",
            )
        )
    )
    password_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[2]/input",
            )
        )
    )

    username_field.send_keys(steam_login)
    password_field.send_keys(steam_password)
    driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[4]/button",
    ).click()

    await asyncio.sleep(10)
    code = await get_verification_code(email, email_password)
    driver.find_element(
            By.XPATH,
            "//input[1]",
        ).send_keys(code)
    WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable(
            (
                By.ID,
                "imageLogin"
            )
        )
    ).click()

    driver.switch_to.window(buff_window)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[21]/div[2]/div[2]/p/a"
            )
        )
    ).click()

    cookie = driver.get_cookies()
    cookie = {c.get('name') : c.get('value') for c in cookie}
    driver.close()
    return cookie
