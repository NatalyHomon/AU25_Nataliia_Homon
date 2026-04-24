import time
import pytest


@pytest.fixture(scope="function")
def track_test_time():
    start = time.time()
    yield
    end = time.time()
    print(f"\nTest execution time: {end - start:.2f} seconds")


@pytest.fixture(scope="session", autouse=True)
def track_suite_time():
    start = time.time()
    yield
    end = time.time()
    print(f"\nWhole suite execution time: {end - start:.2f} seconds")


def add_numbers(a, b):
    return a + b


def test_add_two_positive_numbers(track_test_time):
    a, b = 3, 5
    result = add_numbers(a, b)
    time.sleep(2)
    assert result == 8


def test_add_two_negative_numbers(track_test_time):
    a, b = -3, -5
    result = add_numbers(a, b)
    time.sleep(3)
    assert result == -8


def test_add_negative_and_positive_numbers():
    a, b = -3, 5
    result = add_numbers(a, b)
    time.sleep(10)
    assert result == 2