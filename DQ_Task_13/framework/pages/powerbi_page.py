from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from locators.powerbi_locators import PowerBILocators


class PowerBIPage:
    URL = "https://pbi-plg-edog.analysis-df.windows.net/showcases-gallery/capture-report-views"

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def open(self):
        self.driver.get(self.URL)

    def switch_to_showcase_iframe(self):
        iframe = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe.showcase-frame"))
        )
        self.driver.switch_to.frame(iframe)

    def is_saved_views_button_displayed(self):
        self.switch_to_showcase_iframe()

        element = self.wait.until(
            EC.visibility_of_element_located(PowerBILocators.SAVED_VIEWS_BUTTON)
        )
        return element.is_displayed()

    def is_capture_view_button_displayed(self):
        self.switch_to_showcase_iframe()

        element = self.wait.until(
            EC.visibility_of_element_located(PowerBILocators.CAPTURE_VIEW_BUTTON)
        )
        return element.is_displayed()