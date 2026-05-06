from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.support.relative_locator import locate_with

driver = webdriver.Chrome()
driver.get("https://phptravels.com/demo/")

# First Name
first_name = driver.find_element(By.CSS_SELECTOR, "input.first_name")
first_name.send_keys("Natalia")
driver.save_screenshot("result1.png")



# Last Name
last_name = driver.find_element(By.CSS_SELECTOR, "input.last_name")
last_name.send_keys("Homon")
driver.save_screenshot("result2.png")



#Enter business name

business_name = driver.find_element(By.CLASS_NAME, "company_name")
business_name.send_keys("Junior ")
driver.save_screenshot("result3.png")



#Country
select_element = driver.find_element(By.CLASS_NAME, "country_id")
dropdown = Select(select_element)
dropdown.select_by_index(2)
driver.save_screenshot("result4.png")



#Enter WhatsApp number
whatsapp = driver.find_element(By.XPATH, "//input[@placeholder='Enter WhatsApp number']")
whatsapp.send_keys('12312312321123')
driver.save_screenshot("result5.png")




#email
email = driver.find_element(By.XPATH, "//input[@placeholder='Enter email address']")
email.send_keys("test@example.com")
driver.save_screenshot("result6.png")



#enter number
element = driver.find_element(By.ID, "number")
element.send_keys("8")
driver.save_screenshot("result7.png")



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



# Last Name
last_name = driver.find_element(By.NAME, "lastname")
last_name.send_keys("Homon")
driver.save_screenshot("result10.png")



#relative locators
city = driver.find_element(
    locate_with(By.TAG_NAME, "input")
    .below({By.XPATH: "//h3[text()='Billing Address']"})
)
city.send_keys("Kyiv")
driver.save_screenshot("result11.png")

state = driver.find_element(
    locate_with(By.TAG_NAME, "input")
    .to_right_of({By.ID: "inputCity"})
)
state.send_keys("Kyivska")
driver.save_screenshot("result12.png")

driver.quit()
#3rd website
driver = webdriver.Chrome()
driver.get("https://phptravels.com/blog/")

search_input = driver.find_element(
    By.ID,
    "blog-search"
)
search_input.send_keys("travel")
driver.save_screenshot("result13.png")

link = driver.find_element(
    By.CSS_SELECTOR,
    "a[href='https://phptravels.com/meeting']"
)
link.click()
driver.save_screenshot("result14.png")