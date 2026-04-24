import pytest
import yaml


def get_numbers_data(config_name):
    with open(config_name, "r") as stream:
        config = yaml.safe_load(stream)
    return config["cases"]


def get_case_names(config_name):
    return [case["case_name"] for case in get_numbers_data(config_name)]


def add_numbers(a, b, c):
    if not all(isinstance(x, (int, float)) for x in [a, b, c]):
        raise TypeError("Please check the parameters. All of them must be numeric")
    return a + b + c


@pytest.mark.smoke
@pytest.mark.parametrize(
    "case",
    get_numbers_data("config.yaml"),
    ids=get_case_names("config.yaml")
)
def test_add_numbers(case):
    a, b, c = case["input"]
    expected = case["expected"]

    assert add_numbers(a, b, c) == expected


@pytest.mark.critical
def test_add_invalid_types():
    a, b, c = "a", 2, 1

    with pytest.raises(TypeError):
        add_numbers(a, b, c)