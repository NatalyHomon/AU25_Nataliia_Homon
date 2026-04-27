from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


driver = webdriver.Chrome()

# implicit wait
driver.implicitly_wait(10)


driver.get("https://www.google.com")


try:
    accept_btn = driver.find_element(By.XPATH, "//button[contains(text(),'Accept')]")
    accept_btn.click()
except:
    pass

# 2. Show "Selenium"
search_box = driver.find_element(By.NAME, "q")
search_box.send_keys("Selenium")
search_box.send_keys(Keys.RETURN)

#  explicit wait
wait = WebDriverWait(driver, 15)

first_result = wait.until(
    EC.element_to_be_clickable((By.XPATH, "(//div[@id='search']//a[.//h3])[1]"))
)

first_result.click()

# Open first result
first_result.click()


time.sleep(5)


driver.save_screenshot("google_result.png")

driver.quit()