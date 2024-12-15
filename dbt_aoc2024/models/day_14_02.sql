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
            'test' AS puzzle_type
        UNION ALL
        SELECT
            101 AS width,
            103 AS height,
            'part1' AS puzzle_type

    ),

    iterations AS (
        -- hoping the solution is found within this
        -- range, increase if not
        SELECT row_number() OVER () AS steps
        FROM generate_series(1, 10001) AS t

    ),



    end_position AS (
        SELECT
            *,
            iterations.steps * base.vx + base.x0 AS abs_xt,
            iterations.steps * base.vy + base.y0 AS abs_yt,
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
            CROSS JOIN iterations
        WHERE puzzle_constraints.puzzle_type = 'part1'

    ),

    solution AS (
    -- assuming the solution with the lowest std_dev
    -- is going to be the solution... there could be other
    -- Easter eggs...

        SELECT
            steps,
            stddev(xf) AS std_dev_x,
            stddev(yf) AS std_dev_y
        FROM end_position
        GROUP BY 1
        ORDER BY 2 ASC, 3 ASC
        LIMIT 1
    )

SELECT * FROM solution
