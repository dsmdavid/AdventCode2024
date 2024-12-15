{{
  config(
    materialized = 'table',
    )
}}

WITH
    raw_data AS (
        {% if var('day14', False) %}
         SELECT 
            regexp_split_to_table('p=0,4 v=3,-3
p=6,3 v=-1,-3
p=10,3 v=-1,2
p=2,0 v=2,-1
p=0,0 v=1,3
p=3,0 v=-2,-2
p=7,6 v=-1,-3
p=3,0 v=-1,-2
p=9,3 v=2,3
p=7,3 v=-1,2
p=2,4 v=2,-3
p=9,5 v=-3,-3', '\n') AS raw_
        {% else %}
            SELECT raw_ FROM {{ ref('2024__14') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw_,
            list_transform(
                regexp_extract_all(
                    raw_, '\-?\d+'
                ), x -> try_cast(x AS integer)
            ) AS drones,
            drones[1] AS x0,
            drones[2] AS y0,
            drones[3] AS vx,
            drones[4] AS vy,
            row_number() OVER () AS row_id
        FROM raw_data
    --        where raw_ = 'p=2,4 v=2,-3'
    ),

    puzzle_constraints AS (
        SELECT
            11 AS width,
            7 AS height,
            100 AS steps,
            'test' AS puzzle_type
        UNION ALL
        SELECT
            101 AS width,
            103 AS height,
            100 AS steps,
            'part1' AS puzzle_type

    ),

    end_position AS (
        SELECT
            *,
            puzzle_constraints.steps * base.vx + base.x0 AS abs_xt,
            puzzle_constraints.steps * base.vy + base.y0 AS abs_yt,
            puzzle_constraints.width // 2 AS mid_line_x,
            puzzle_constraints.height // 2 AS mid_line_y,
            abs_xt % puzzle_constraints.width AS endx,
            abs_yt % puzzle_constraints.height AS endy,
            CASE
                WHEN endx >= 0 THEN endx
                ELSE puzzle_constraints.width + endx
            END AS xf,
            CASE
                WHEN endy >= 0 THEN endy
                ELSE puzzle_constraints.height + endy
            END AS yf
        FROM base
            CROSS JOIN puzzle_constraints
        WHERE puzzle_constraints.puzzle_type = 'part1'

    ),

    quadrants AS (
        SELECT
            *,
            CASE
                WHEN xf < mid_line_x AND yf < mid_line_y THEN 'Q1'
                WHEN xf > mid_line_x AND yf < mid_line_y THEN 'Q2'
                WHEN xf < mid_line_x AND yf > mid_line_y THEN 'Q3'
                WHEN xf > mid_line_x AND yf > mid_line_y THEN 'Q4'
            END AS quadrant
        FROM end_position
    ),

    drone_count AS (

        SELECT
            quadrant,
            count(*) AS n_drones
        FROM quadrants
        WHERE quadrant IS NOT NULL
        GROUP BY quadrant
    )

SELECT
    list_reduce(
        array_agg(n_drones), (x, y) -> x * y
    ) AS res1
FROM drone_count
