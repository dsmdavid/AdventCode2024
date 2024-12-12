import sys
import os
from typing import List
from math import log, floor, log10
import functools

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from utils.log import MyLogger
from utils.tools import get_input

logger = MyLogger(log_file="aoc2024.log", log_path="logs", name=__name__)
input_1 = "125 17"

sample_input = f"""{ input_1 }""".split("\n")


def transform_number(number: int) -> List[int]:
    if number == 0:
        return [1]
    elif floor(log(number, 10) + 1) % 2 == 0:
        return [
            int(number // pow(10, floor(log(number, 10) + 1) / 2)),
            int(
                number
                - pow(10, floor(log(number, 10) + 1) / 2)
                * (number // pow(10, floor(log(number, 10) + 1) / 2))
            ),
        ]
    else:
        return [number * 2024]


input_day = get_input("2024__11")
input_to_use = input_day
ans1 = ans2 = 0

numbers = list(map(int, input_to_use[0].split(" ")))

iteration_outputs = {0: numbers}
it = 1
for iteration in range(0, 25):
    l = []
    while numbers:
        l.extend(transform_number(numbers.pop()))
    numbers = l[:]
    it += 1

print("Part1\t", len(numbers))

numbers = list(map(int, input_to_use[0].split(" ")))


@functools.cache
def get_number_of_components(number, iterations_left):
    if iterations_left == 0:
        return 1
    if number == 0:
        return get_number_of_components(1, iterations_left - 1)

    n_digits = floor(log(number, 10) + 1)
    if n_digits % 2 == 0:
        left_half = number // pow(10, n_digits / 2)
        right_half = number - pow(10, n_digits / 2) * (
            number // pow(10, n_digits / 2)
        )  ## this is just modulo

        return get_number_of_components(
            left_half, iterations_left - 1
        ) + get_number_of_components(right_half, iterations_left - 1)
    return get_number_of_components(number * 2024, iterations_left - 1)


print("Part2\t", sum([get_number_of_components(n, 75) for n in numbers]))
