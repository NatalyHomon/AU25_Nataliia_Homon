from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    """
    Write your code below
    """
    seen = set()

    for number in array:
        needed = target - number

        if needed in seen:
            return [needed, number]

        seen.add(number)
    return []

print(task_1([-1, -1], 2))


def task_2(number: int) -> int:
    """
    Write your code below
    """
    sign = 1
    if number < 0:
        sign = -1
        number = -number
    result = 0
    while number > 0:
        digit = number % 10
        result = result * 10 + digit
        number = number // 10

    return result * sign
print(task_2(120))

def task_3(array: List[int]) -> int:
    """
    Write your code below
    """
    for x in array:
        index = abs(x) - 1

        if array[index] < 0:
            return abs(x)

        array[index] = -array[index]

    return -1
print(task_3([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5]))



def task_4(string: str) -> int:
    """
    Write your code below
    """
    ss = string
    values = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }

    total = 0

    for i in range(len(ss)):
        current = values[ss[i]]

        if i + 1 < len(ss) and current < values[ss[i + 1]]:
            total -= current
        else:
            total += current

    return total

print(task_4('I'))

def task_5(array: List[int]) -> int:
    """
    Write your code below
    """
    nums = array
    if not nums:
        return None  # або можна кинути помилку, якщо список не може бути порожнім

    smallest = nums[0]

    for num in nums:
        if num < smallest:
            smallest = num

    return smallest
print(task_5([0]))