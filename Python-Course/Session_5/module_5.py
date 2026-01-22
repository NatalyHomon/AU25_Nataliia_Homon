from collections import Counter
import os
from pathlib import Path
from random import choice
from random import seed
from typing import List, Union
#urllib.error import HTTPError
import certifi
import requests
from requests.exceptions import ConnectionError, RequestException
#from gensim.utils import simple_preprocess
import re


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"


def task_1():
    seed(1)
    with open(PATH_TO_NAMES, "r", encoding="utf-8") as names,\
        open(PATH_TO_SURNAMES, "r", encoding="utf-8") as surnames,\
        open(PATH_TO_OUTPUT, "w", encoding="utf-8") as output:

        names = sorted(name.strip().lower() for name in names if name.strip())
        surnames = [surname.strip().lower() for surname in surnames if surname.strip()]
        full_names = []
        for name in names:
            random_surname = choice(surnames)
            full_names.append(f"{name.lower()} {random_surname}\n")

        output.writelines(full_names)



def task_2(top_k: int):
    with open(PATH_TO_TEXT, "r", encoding="utf-8") as f_text,\
        open(PATH_TO_STOP_WORDS, "r", encoding="utf-8") as f_stopwords:
        stopwords = {w.strip().lower() for w in f_stopwords if w.strip()}

        text = f_text.read().lower()
        words = re.findall(r"[a-z]+", text)
        cleaned = [w for w in words if w.isalpha() and w not in stopwords]

        return Counter(cleaned).most_common(top_k)



def task_3(url: str):
    try:
        response = requests.get(
            url,
            verify=certifi.where(),
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        response.raise_for_status()
        return response
    except RequestException as e:
        # підняти САМЕ RequestException (не HTTPError)
        raise RequestException(str(e))



def task_4(data: List[Union[int, str, float]]):
    result =0
    for d in data:
        try:
           result += d
        except TypeError:
           result += float(d)
    return result


def task_5():

    try:
        a, b = input().split()
        a = float(a)
        b = float(b)
        result = a/b
    except ZeroDivisionError:
        print("Can't divide by zero")
    except (ValueError):
        print("Entered value is wrong")
    else:
        if result.is_integer():
            print(int(result))
        else:
            print(f"{result:.3f}")


