from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

driver_path = BASE_DIR / "drivers" / "geckodriver.exe"


def open_google_in_chrome():
    chrome_driver = webdriver.Chrome()
    chrome_driver.get("https://www.google.com")
    print("Chrome title:", chrome_driver.title)
    time.sleep(5)
    chrome_driver.quit()


def open_google_in_firefox():
    firefox_service = FirefoxService(
        executable_path=str(driver_path)
    )
    options = Options()
    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"


    firefox_driver = webdriver.Firefox(
        service=firefox_service,
        options=options
    )
    firefox_driver.get("https://www.google.com")
    print("Firefox title:", firefox_driver.title)
    time.sleep(5)
    firefox_driver.quit()


open_google_in_chrome()
open_google_in_firefox()