import sys
import os
from typing import List, Tuple
from collections import defaultdict, deque
from math import inf
import functools

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from utils.log import MyLogger
from utils.tools import get_input

logger = MyLogger(log_file="aoc2024.log", log_path="logs", name=__name__)
input_1 = """###############
#.......#....E#
#.#.###.#.###.#
#.....#.#...#.#
#.###.#####.#.#
#.#.#.......#.#
#.#.#####.###.#
#...........#.#
###.#.#####.#.#
#...#.....#.#.#
#.#.#.###.#.#.#
#.....#...#.#.#
#.###.#.#.#.#.#
#S..#.....#...#
###############"""

input_2 = """#################
#...#...#...#..E#
#.#.#.#.#.#.#.#.#
#.#.#.#...#...#.#
#.#.#.#.###.#.#.#
#...#.#.#.....#.#
#.#.#.#.#.#####.#
#.#...#.#.#.....#
#.#.#####.#.###.#
#.#.#.......#...#
#.#.###.#####.###
#.#.#...#.....#.#
#.#.#.#####.###.#
#.#.#.........#.#
#.#.#.#########.#
#S#.............#
#################"""

sample_input = f"""{ input_1 }""".split("\n")

GRID = {}
GRID_SCORES = defaultdict(lambda: inf)


class Coordenate(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __add__(self, other):
        return Coordenate(x=self.x + other.x, y=self.y + other.y)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __repr__(self):
        return f"({self.x},{self.y})"

    def val_(self):
        return (self.x, self.y)


class Route(Coordenate):
    def __init__(self, x, y, direction=Tuple, score=0, route_path=[]):
        super().__init__(x, y)
        self.direction = direction
        self.score = score
        self.route_path = route_path[:]

    def __repr__(self):
        return f"({self.x},{self.y}, {self.direction}, {self.score}, {self.route_path[0:5]})"

    def move(self, next_position, score_cost):

        new_path = self.route_path[:]

        if (self.x, self.y) == exit:
            HOLDER.append((self.route_path[:], self.score))
            return None
        potential_move = self + Coordenate(*next_position)
        if GRID.get(potential_move.val_(), "#") == "#":
            return None
        if (
            max_grid_height > potential_move.y
            and max_grid_width > potential_move.y
            and potential_move.x > 0
            and potential_move.y > 0
        ):
            if self.score + score_cost <= GRID_SCORES[potential_move.val_()] + 1001:
                GRID_SCORES[potential_move.val_()] = self.score + score_cost
                next_position_vals = (
                    self.x + next_position[0],
                    self.y + next_position[1],
                )
                new_path.append(next_position_vals)
                return Route(
                    x=next_position_vals[0],
                    y=next_position_vals[1],
                    direction=next_position,
                    score=self.score + score_cost,
                    route_path=new_path,
                )
        else:
            return None

    def options(self):
        return list(
            filter(
                lambda x: x is not None,
                [self.move(self.direction, 1)]
                + [self.move(nd, 1001) for nd in DIRECTION_ROTATE[self.direction]],
            )
        )


DIRECTION_ROTATE = {
    (1, 0): [(0, 1), (0, -1)],
    (-1, 0): [(0, 1), (0, -1)],
    (0, 1): [(1, 0), (-1, 0)],
    (0, -1): [(1, 0), (-1, 0)],
}

HOLDER = []


def get_grid(input_raw: List) -> Tuple:
    for i, vals in enumerate(input_raw):
        for k, char_ in enumerate(vals):
            GRID[(k, i)] = char_
            if char_ == "S":
                start = (k, i)
            if char_ == "E":
                exit = (k, i)
    return i, k, start, exit


input_day = get_input("2024__16")
input_to_use = input_day
max_grid_height, max_grid_width, start, exit = get_grid(input_to_use)
ans1 = ans2 = 0
# print(max_grid_height, max_grid_width)
# print(start, exit)

s = Route(*start, direction=(1, 0), score=0, route_path=[start])
paths = deque([s])
while paths:
    t = paths.popleft()
    _ = t.options()
    paths.extend(_)

target_score = GRID_SCORES[exit]
print("Part1\t", target_score)

cells = []
candidate_paths = list(filter(lambda x: x[1] == target_score, HOLDER))
for valid_path in candidate_paths:
    cells.extend(valid_path[0])

print("Part2\t", len(set(cells)))
# print(cells)

# print grid
# update grid:
# for item in cells:
#     GRID[item] = 'O'

# for i in range(0, max_grid_height+1):
#     for k in range(0, max_grid_width+1):
#         print(GRID[(k,i)], end='')
#     print('\n', end='')
