# from collections import defaultdict as dd
# from itertools import product
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    for key, value in data_1.items():
        if key in data_2:
            data_2[key] = data_2[key] + value
        else:
            data_2[key] = value
    return data_2

result =task_1({"a": 123, "b": 23, "c": 0}, {'a' : 1, 'b': 11, 'd': 99})
print(result)

def task_2():

    num=15
    result = {}
    for num in range(1, num+1):
        result[num] = num * num
    return result

print(task_2())


def task_3(data: Dict[Any, List[str]]):
    result = [""]
    for letters in data.values():
        new_result = []
        for prefix in result:
            for letter in letters:
                new_result.append(prefix + letter)
        result = new_result
    return result

print(task_3({"1": ["a", "b"], "2": ["c", "d"]}))



def task_4(data: Dict[str, int]):
    if not data:
        return {}
    sorted_items = sorted(data.items(), key=lambda item: item[1], reverse =True)
    return [key for key, value in sorted_items[:3]]
print(task_4({'a': -1, 'b': 5874, 'c': 560, 'd': -30}))

def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    result = {}
    for item in data:
        if item[0] not in result:
            result[item[0]] = [item[1]]
        else:
            result[item[0]] += [item[1]]
    return result

print(task_5([('yellow', 1), ('blue', 2), ('yellow', 3), ('blue', 4), ('red', 1)]))


def task_6(data: List[Any]):
    result = []
    for item in data:
        if item not in result:
            result.append(item)
    return result

print(task_6(["1", "2", 1, 1, 2, 2]))


def task_7(words: [List[str]]) -> str:
    if not words:
        return ''

    prefix = words[0]

    for i in range(len(prefix)):
        char = prefix[i]
        for word in words[1:]:
            if i >= len(word) or word[i] != char:
                return prefix[:i]
    return prefix

print(task_7(["sun", "recap"]))


def task_8(haystack: str, needle: str) -> int:
    if not needle:
        return 0
    result = haystack.find(needle)
    return result

print(task_8("Star Killer", ""))
