from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
import time


def open_google_in_chrome():
    chrome_driver = webdriver.Chrome()
    chrome_driver.get("https://www.google.com")
    print("Chrome title:", chrome_driver.title)
    chrome_driver.quit()


def open_google_in_firefox():
    firefox_service = FirefoxService(
        executable_path="drivers/geckodriver.exe"
    )
    # Required because Firefox is not detected automatically
    options = Options()
    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"


    firefox_driver = webdriver.Firefox(
        service=firefox_service,
        options=options
    )
    firefox_driver.get("https://www.google.com")
    print("Firefox title:", firefox_driver.title)
    firefox_driver.quit()


open_google_in_chrome()
open_google_in_firefox()