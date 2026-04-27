from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time

driver = webdriver.Chrome()
driver.get("https://phptravels.com/demo/")

# First Name
first_name = driver.find_element(By.CSS_SELECTOR, "input.first_name")
first_name.send_keys("Natalia")
driver.save_screenshot("result1.png")

time.sleep(2)

# Last Name
last_name = driver.find_element(By.CSS_SELECTOR, "input.last_name")
last_name.send_keys("Homon")
driver.save_screenshot("result2.png")

time.sleep(2)

#Enter business name

business_name = driver.find_element(By.CLASS_NAME, "company_name")
business_name.send_keys("Junior ")
driver.save_screenshot("result3.png")

time.sleep(2)

#Country
select_element = driver.find_element(By.CLASS_NAME, "country_id")
dropdown = Select(select_element)
dropdown.select_by_index(2)
driver.save_screenshot("result4.png")

time.sleep(2)

#Enter WhatsApp number
whatsapp = driver.find_element(By.XPATH, "//input[@placeholder='Enter WhatsApp number']")
whatsapp.send_keys('12312312321123')
driver.save_screenshot("result5.png")


time.sleep(2)

#email
email = driver.find_element(By.XPATH, "//input[@placeholder='Enter email address']")
email.send_keys("test@example.com")
driver.save_screenshot("result6.png")

time.sleep(2)

#enter number
element = driver.find_element(By.ID, "number")
element.send_keys("8")
driver.save_screenshot("result7.png")

time.sleep(2)

#button
button = driver.find_element(By.ID, "demo")
driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
driver.save_screenshot("result8.png")

driver.quit()


driver = webdriver.Chrome()
driver.get("https://phptravels.org/register.php")


# First Name
first_name = driver.find_element(By.NAME, "firstname")
first_name.send_keys("Natalia")
driver.save_screenshot("result9.png")

time.sleep(2)

# Last Name
last_name = driver.find_element(By.NAME, "lastname")
last_name.send_keys("Homon")
driver.save_screenshot("result10.png")

driver.quit()