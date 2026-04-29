from pathlib import Path
import pytest
import yaml
import allure



BASE_DIR = Path(__file__).parent
SQL_CONFIG = BASE_DIR / "sql_tests_config.yaml"


def get_tests(test_type):
    with open(SQL_CONFIG, "r") as file:
        config = yaml.safe_load(file)
    return config[test_type]


def get_test_names(test_type):
    return [test["name"] for test in get_tests(test_type)]


@pytest.mark.smoke
@pytest.mark.parametrize(
    "test_case",
    get_tests("smoke_tests"),
    ids=get_test_names("smoke_tests")
)
def test_smoke_db_objects(db_cursor, test_case):
    with allure.step(f"Execute SQL query: {test_case['sql']}"):
        db_cursor.execute(test_case["sql"])
        actual_result = db_cursor.fetchone()[0]

    with allure.step("Check actual result equals expected result"):
        assert actual_result == test_case["expected"]


@pytest.mark.critical
@pytest.mark.parametrize(
    "test_case",
    get_tests("critical_tests"),
    ids=get_test_names("critical_tests")
)
def test_critical_db_data(db_cursor, test_case):
    with allure.step(f"Execute SQL query: {test_case['sql']}"):
        db_cursor.execute(test_case["sql"])
        actual_result = db_cursor.fetchone()[0]

    with allure.step("Check critical expected result"):
        if "expected" in test_case:
            assert actual_result == test_case["expected"]
        elif "expected_min" in test_case:
            assert actual_result >= test_case["expected_min"]