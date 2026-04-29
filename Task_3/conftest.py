from pathlib import Path
import pytest
import yaml


import psycopg2


BASE_DIR = Path(__file__).parent
DB_CONFIG_PATH = BASE_DIR / "db_config.yaml"


def read_yaml(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


@pytest.fixture(scope="session")
def db_cursor():
    config = read_yaml(DB_CONFIG_PATH)

    db_type = config["db_type"]

    if  db_type == "postgres":
        db = config["postgres"]

        connection = psycopg2.connect(
            database=db["database"],
            user=db["user"],
            password=db["password"],
            host=db["host"],
            port=db["port"]
        )

    else:
        raise ValueError("Unsupported DB type")

    cursor = connection.cursor()

    yield cursor

    cursor.close()
    connection.close()



