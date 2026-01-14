import time
from typing import List


Matrix = List[List[int]]


def task_1(exp: int):
    def inner(num):
        return num ** exp
    return inner

power = task_1(3)
print(power(3))
print(power(4))
print(power(0))


def task_2(*args, **kwargs):
    result = list(args) + list(kwargs.values())
    for item in result:
        print(item)

task_2(1, 3, moment=4, cap="arkadiy")


def helper(func):
    def wrapper(*args):
        print("Hi, friend! What's your name?")
        result = func(*args)
        print("See you soon!")
        return result

    return wrapper


@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")

task_3("John")
task_3("mike")

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        run_time = end - start
        print(f"Finished {func.__name__} in {run_time:.4f} secs")
        return result
    return wrapper


@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])
task_4()

def task_5(matrix: Matrix) -> Matrix:
    result = []

    cols = len(matrix[0])

    for c in range(cols):
        new_row = []
        for r in range(len(matrix)):
            new_row.append(matrix[r][c])
        result.append(new_row)

    return result


print(task_5(matrix=[[1,2,3],[4,5,6], [7,8,9]]))

def task_6(queue: str):
    balance = 0

    for ch in queue:
        if ch == "(":
            balance += 1
        elif ch == ")":
            balance -= 1
            if balance < 0:
                return False

    return balance == 0

print(task_6("((()))"))
