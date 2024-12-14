{{
  config(
    materialized = 'table',
    )
}}
-- the output of this model is the path followed 
-- by the guard
WITH RECURSIVE
    raw_data AS (
        {% if var('day13', False) %}
         SELECT 
            regexp_split_to_table('Button A: X+94, Y+34
Button B: X+22, Y+67
Prize: X=8400, Y=5400

Button A: X+26, Y+66
Button B: X+67, Y+21
Prize: X=12748, Y=12176

Button A: X+17, Y+86
Button B: X+84, Y+37
Prize: X=7870, Y=6450

Button A: X+69, Y+23
Button B: X+27, Y+71
Prize: X=18641, Y=10279', '\n') AS raw_
        {% else %}
            SELECT raw_ FROM {{ ref('2024__13') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw_,
            ROW_NUMBER() OVER () AS row_id
        FROM raw_data
    ),

    claw_as_row AS (
        SELECT
            row_id // 4 AS block,
            LIST_TRANSFORM(
                REGEXP_EXTRACT_ALL(
                    ARRAY_TO_STRING(LIST(raw_ ORDER BY row_id), '#'),
                    '\+?\d+'
                ), x -> TRY_CAST(x AS hugeint)
            )
                AS items,
            items[1] AS ax_,
            items[2] AS ay_,
            items[3] AS bx_,
            items[4] AS by_,
            items[5] AS zx_,
            items[6] AS zy_
        FROM base
        WHERE raw_ != ''
        GROUP BY block
    ),

    solutions AS (

        SELECT
            *,
            TRY_CAST(
                (
                    (ax_ * zy_ - ay_ * zx_)
                    /
                    (by_ * ax_ - bx_ * ay_
                    )
                ) AS hugeint)
                AS b,
            TRY_CAST(
                (
                    zx_ - b * bx_
                ) / ax_
                AS hugeint
            ) AS a,
            a * ax_ + b * bx_ = zx_
            AND a * ay_ + b * by_ = zy_
                AS solved,

            solved AND a <= 100 AND b <= 100 AS valid_solutions
        FROM claw_as_row

    ),

    claw_as_row_part_2 AS (
        SELECT
            block,
            ax_,
            ay_,
            bx_,
            by_,
            zx_ + 10000000000000 AS zx_,
            zy_ + 10000000000000 AS zy_
        FROM claw_as_row
    ),

    solutions2 AS (

        SELECT
            *,
            TRY_CAST(
                (
                    (ax_ * zy_ - ay_ * zx_)
                    /
                    (by_ * ax_ - bx_ * ay_
                    )
                ) AS hugeint)
                AS b,
            TRY_CAST(
                (
                    zx_ - b * bx_
                ) / ax_
                AS hugeint
            ) AS a,

            a * ax_ + b * bx_ = zx_
            AND a * ay_ + b * by_ = zy_
                AS solved,
            solved AND TRUE -- AND a <= 100 AND b <= 100 
                AS valid_solutions
        FROM claw_as_row_part_2

    )

SELECT
    'part1' AS part_,
    SUM(a * 3 + b) AS result
FROM solutions
WHERE valid_solutions

UNION ALL

SELECT
    'part2' AS part_,
    SUM(a * 3 + b) AS result
FROM solutions2
WHERE valid_solutions
