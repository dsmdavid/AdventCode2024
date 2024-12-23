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
input_1 = """5,4
4,2
4,5
3,0
2,1
6,3
2,4
1,5
0,6
3,3
2,6
5,1
1,2
5,5
2,5
6,5
1,4
0,4
6,4
1,1
6,1
1,0
0,5
1,6
2,0"""

sample_input = f"""{ input_1 }""".split("\n")


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
    def __init__(
        self,
        x,
        y,
        direction=Tuple,
        score=0,
        route_path=[],
        grid={},
        grid_scores=defaultdict(lambda: inf),
    ):
        super().__init__(x, y)
        self.direction = direction
        self.score = score
        self.route_path = route_path[:]
        self.grid = grid
        self.grid_scores = grid_scores

    def __repr__(self):
        return f"({self.x},{self.y}, {self.direction}, {self.score}, {self.route_path[0:5]})"

    def move(self, next_position, score_cost):

        new_path = self.route_path[:]
        if (self.x, self.y) == exit:
            HOLDER.append((self.route_path[:], self.score))
            return None
        potential_move = self + Coordenate(*next_position)

        if self.grid.get(potential_move.val_(), "#") == "#":
            return None
        if (
            max_grid_height + 1 > potential_move.y
            and max_grid_width + 1 > potential_move.y
            and potential_move.x >= 0
            and potential_move.y >= 0
        ):

            if self.score + score_cost < self.grid_scores[potential_move.val_()]:
                # print(self.score + score_cost)
                self.grid_scores[potential_move.val_()] = self.score + score_cost
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
                    grid=self.grid,
                    grid_scores=self.grid_scores,
                )
        else:
            return None

    def options(self):
        return list(
            filter(
                lambda x: x is not None,
                [self.move(self.direction, 1)]
                + [self.move(nd, 1) for nd in DIRECTION_ROTATE[self.direction]],
            )
        )


DIRECTION_ROTATE = {
    (1, 0): [(0, 1), (0, -1)],
    (-1, 0): [(0, 1), (0, -1)],
    (0, 1): [(1, 0), (-1, 0)],
    (0, -1): [(1, 0), (-1, 0)],
}

HOLDER = []


def get_max_min(input_raw):
    start = (0, 0)
    if len(input_raw) > 100:
        ## day input
        exit_ = (70, 70)
    else:
        # sample input
        exit_ = (6, 6)

    return start, exit_


def get_grid(input_raw: List, steps=None, grid={}, exit=(6, 6)) -> Tuple:

    for i in range(exit[0] + 1):
        for k in range(exit[1] + 1):
            grid[(k, i)] = "."
    if steps:
        for pos in range(steps):
            grid[tuple(list(map(int, input_raw[pos].split(","))))] = "#"
    return i, k


def min_reach_exit(input_to_use: List, steps: int) -> int:
    paths = deque([])
    new_grid = {}
    new_grid_scores = defaultdict(lambda: inf)
    _, _ = get_grid(input_to_use, steps=steps, grid=new_grid, exit=exit_)
    for start_direction in [(0, 1), (1, 0), (-1, 0), (0, -1)]:
        r = Route(
            *start,
            direction=start_direction,
            score=0,
            route_path=[start],
            grid=new_grid,
            grid_scores=new_grid_scores,
        )
        paths.append(r)

    ct = 0
    while paths:
        t = paths.popleft()
        _ = t.options()
        paths.extend(_)
        ct += 1
        if ct > 100000:
            break

    target_score = new_grid_scores[exit_]
    return target_score


def solver(
    input_to_use: List,
    steps: int,
    min_steps: int,
    max_steps: int,
    iterations: int,
    run: int,
    start: Tuple,
    exit: Tuple,
):

    target_score = min_reach_exit(input_to_use, steps)

    if steps == min_steps or steps == max_steps:
        print(f"returning {steps} at run {run}, cond_1. {min_steps,max_steps}")
        return steps
    if target_score == inf:
        if steps < min_steps:
            print(f"returning {steps} at run {run}, cond_2")
            return steps
        next_steps = int(min_steps + (steps - min_steps) / 2)
        return solver(
            input_to_use,
            next_steps,
            min_steps,
            steps,
            iterations,
            run + 1,
            start,
            exit_,
        )
    else:
        # we haven't blocked it yet
        if steps > max_steps:
            print(f"returning {steps} at run {run}, cond_3")

            return steps
        next_steps = int(steps + (max_steps - steps) / 2)
        return solver(
            input_to_use,
            next_steps,
            steps,
            max_steps,
            iterations,
            run + 1,
            start,
            exit_,
        )


input_day = get_input("2024__18")
input_to_use = input_day
max_steps = len(input_to_use)
min_steps = steps = 12 if max_steps < 100 else 1024
print("Starting steps\t", steps)
start, exit_ = get_max_min(input_to_use)


max_grid_height, max_grid_width = get_grid(input_to_use, exit=exit_)
print(exit)
ans1 = ans2 = 0


t2 = solver(
    input_to_use,
    steps,
    min_steps,
    max_steps,
    iterations=100000,
    run=1,
    start=start,
    exit=exit_,
)

print("Part1\t", min_reach_exit(input_to_use, 12 if len(input_to_use) < 100 else 1024))
print("Part2\t", input_to_use[t2])
