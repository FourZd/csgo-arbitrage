from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import imaplib
import re
from selenium.webdriver.firefox.options import Options
from utilities import try_decorator

def get_verification_code(email, email_password):
    mail = imaplib.IMAP4_SSL("imap.rambler.ru", 993)

    mail.login(email, email_password)
    mail.select("inbox")
    time.sleep(10)
    search_criteria = 'FROM "Steam Support" SUBJECT "Your Steam account: Access from new web or mobile device"'
    result, data = mail.search(None, search_criteria)
    latest_email = data[0].split()[-1]
    result, data = mail.fetch(latest_email, "(RFC822)")
    raw_email = data[0][1]
    raw_email_string = raw_email.decode("utf-8")
    code = re.findall(r"Login Code\s+(\w+)", raw_email_string)[0]
    return code

def get_firefox_options(proxy, proxy_username, proxy_password):
    proxy_host = re.findall(r"^(.*?):", proxy)[0]
    proxy_port = re.findall(r":\s*(.*)", proxy)[0]
    print(proxy_host)
    print(proxy_port)
    options = Options()
    options.set_preference('network.proxy.type', 1)
    options.set_preference('network.proxy.http', proxy_host)
    options.set_preference('network.proxy.http_port', proxy_port)
    options.set_preference('network.proxy.ssl', proxy_host)
    options.set_preference('network.proxy.ssl_port', proxy_port)
    options.set_preference('network.proxy.socks_username', proxy_username)
    options.set_preference('network.proxy.socks_password', proxy_password)
    return options


@try_decorator
def click_element(element):
    return element.click()
@try_decorator
def get_page(driver, page):
    return driver.get(page)

@try_decorator
def find_element(driver, condition, element):
    return driver.find_element(condition, element)
@try_decorator
def driver_wait_until(driver, seconds, until):
    """Waits until some condition and then returns object if exists"""
    object = WebDriverWait(driver, seconds).until(until)
    return object
def login(steam_login, steam_password, email, email_password, options):
    
    driver = webdriver.Firefox(options=options)

    
    get_page(driver, "https://buff.163.com/")
    login_button = find_element(driver, By.XPATH, "/html/body/div[1]/div/div[3]/ul/li/a")
    click_element(login_button)
    remember_me = driver_wait_until(driver, 10, until=EC.element_to_be_clickable((By.ID, "remember-me")))
    click_element(remember_me)

    login_steam = driver_wait_until(driver, 10, EC.element_to_be_clickable((By.ID, "j_login_other")))
    click_element(login_steam)

    buff_window = driver.current_window_handle
    for window in driver.window_handles:
        if window != buff_window:
            driver.switch_to.window(window)
            break
    steam_login_window = driver.current_window_handle
    # Wait for username and password fields to be clickable
    
    username_field = driver_wait_until(driver, 30,
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[1]/input",
            )
        )
    )
    password_field = driver_wait_until(driver, 30,
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[2]/input",
            )
        )
    )

    username_field.send_keys(steam_login)
    password_field.send_keys(steam_password)
    login_button = find_element(driver, By.XPATH,
        "/html/body/div[1]/div[7]/div[4]/div[1]/div[1]/div/div/div/div[2]/div/form/div[4]/button",
    )
    login_button.click()
    try:
        code = get_verification_code(email, email_password)
        verification_field = find_element(driver,
            By.XPATH,
            "//input[1]",
        )
        verification_field.send_keys(code)
    except Exception as e:
        print('Not rambler mail, continying without a code...', email, e)
    login = driver_wait_until(driver, 15,
        EC.element_to_be_clickable(
            (
                By.ID,
                "imageLogin"
            )
        )
    )
    click_element(login)

    driver.switch_to.window(buff_window)
    driver.refresh()

    cookie = driver.get_cookies()
    print(cookie)
    cookie = {c.get('name') : c.get('value') for c in cookie}
    driver.close()
    return cookie
