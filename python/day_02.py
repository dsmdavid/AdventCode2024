import sys
import os
from typing import List
import functools

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from utils.log import MyLogger
from utils.tools import get_input

logger = MyLogger(log_file="aoc2024.log", log_path="logs", name=__name__)

sample_input = """7 6 4 2 1
1 2 7 8 9
9 7 6 2 1
1 3 2 4 5
8 6 4 4 1
1 3 6 7 9""".split(
    "\n"
)

def valid_numbers(x: int, y: int) -> bool:
    return (1 <= abs(x-y) <= 3)

def valid_report(report: List) -> bool:
    # ascending or descending
    valid_ascending_descending = (report == sorted(report) or report == list(reversed(sorted(report))))
    # diff with prior > 1 <= 3
    valid_distance = functools.reduce(lambda x, y: y * (x != 0 and valid_numbers(x,y)), report) > 0
    return valid_ascending_descending * valid_distance

def safe_report(report: List) -> bool:
    flag = False

    counter = 0
    flag = valid_report(report)
    while not flag and counter <= len(report):
        tmp_report = report[0:counter] + report[counter+1:]
        flag = valid_report(tmp_report)
        counter += 1
    return flag

input_day = get_input("2024__2")
input_to_use = input_day
reports = [list(map(int, x.split())) for x in input_to_use]

part_1 = sum(valid_report(report) for report in reports)
part_2 = sum(safe_report(report) for report in reports)
print(part_1, part_2)