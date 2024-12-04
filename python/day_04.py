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

TARGET_WORD = "XMAS"
ADJACENT = list((x, y) for x in (-1, 0, 1) for y in (-1, 0, 1))
sample_input = """MMMSXXMASM
MSAMXMSMSA
AMXSXMAAMM
MSAMASMSMX
XMASAMXAMM
XXAMMXXAMA
SMSMSASXSS
SAXAMASAAA
MAMMMXMMMM
MXMXAXMASX""".split(
    "\n"
)


def check_valid(current_val: str, target_val: str) -> bool:
    return current_val == target_val[0 : len(current_val)]


input_day = get_input("2024__4")
input_to_use = input_day

MAP = {(x, y): ch for x, row in enumerate(input_to_use) for y, ch in enumerate(row)}
# XMAS starts with X
starting_positions = [pos for pos, val in MAP.items() if val == "X"]
tracks = [[pos] for pos in starting_positions]

counter = 0
valid_tracks = []
# start by adding the direction of growth (there doesn't seem to
# be turns allowed, so words are consecutive letters in the same
# direction)
for item in ADJACENT:
    for track in tracks:
        valid_tracks.append([item] + track)

while counter < len(TARGET_WORD) - 1:
    next_ = []
    for track in valid_tracks:
        next_tracks = []
        if len(track) <= len(TARGET_WORD):
            if MAP.get(track[-1], "") == TARGET_WORD[counter]:
                # track[0] holds the "direction of growth"
                next_position = (track[-1][0] + track[0][0], track[-1][1] + track[0][1])
                _ = track + [next_position]
                next_tracks.append(_)
        next_.extend(next_tracks)
    valid_tracks = next_

    counter += 1

print("part1", "\t", len([item for item in valid_tracks if MAP.get(item[-1]) == "S"]))

## part 2


def valid_x_mas(position):
    # (-1,-1) =='M' and (-1,1) == 'S' and (1,-1) == 'M' and (1,1) == 'S'
    # (-1,-1) =='M' and (-1,1) == 'M' and (1,-1) == 'S' and (1,1) == 'S'
    # (-1,-1) =='S' and (-1,1) == 'S' and (1,-1) == 'M' and (1,1) == 'M'
    # (-1,-1) =='S' and (-1,1) == 'M' and (1,-1) == 'S' and (1,1) == 'M'
    ul = (position[0] - 1, position[1] - 1)
    ur = (position[0] - 1, position[1] + 1)
    bl = (position[0] + 1, position[1] - 1)
    br = (position[0] + 1, position[1] + 1)

    valid = True
    if MAP.get(ul, "") == "M":
        valid = MAP.get(br, "") == "S"
    elif MAP.get(ul, "") == "S":
        valid = MAP.get(br, "") == "M"
    else:
        valid = False

    if MAP.get(ur, "") == "M":
        valid = valid * MAP.get(bl, "") == "S"
    elif MAP.get(ur, "") == "S":
        valid = valid * MAP.get(bl, "") == "M"
    else:
        valid = False
    return valid


# XMAS cross centered at A
starting_positions = [pos for pos, val in MAP.items() if val == "A"]

print("part2", "\t", len([pos for pos in starting_positions if valid_x_mas(pos)]))
