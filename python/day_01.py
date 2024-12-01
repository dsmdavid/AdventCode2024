import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from utils.log import MyLogger
from utils.tools import get_input

logger = MyLogger(log_file="aoc2024.log", log_path="logs", name=__name__)

sample_input = """3   4
4   3
2   5
1   3
3   9
3   3""".split(
    "\n"
)


input_day = get_input("2024__01")
input_to_use = input_day
a,b = list(map(list, zip(*[list(map(int, x.split())) for x in input_to_use])))
# part 1 - sort and compare
part_1 = sum([abs(x[0]-x[1]) for x in list(zip(sorted(a), sorted(b)))])
part_2 = sum([x * b.count(x) for x in a])
print(part_1, part_2)