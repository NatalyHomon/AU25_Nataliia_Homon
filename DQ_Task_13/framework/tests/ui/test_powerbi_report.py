from pages.powerbi_page import PowerBIPage


def test_capture_view_button_is_displayed(driver):
    page = PowerBIPage(driver)

    page.open()

    assert page.is_capture_view_button_displayed()


def test_saved_views_button_is_displayed(driver):
    page = PowerBIPage(driver)

    page.open()

    assert page.is_saved_views_button_displayed()