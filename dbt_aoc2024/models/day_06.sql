{{
  config(
    materialized = 'table',
    )
}}
-- the output of this model is the path followed 
-- by the guard
WITH RECURSIVE
    raw_data AS (
        {% if var('day06', False) %}
         SELECT 
            regexp_split_to_table('....#.....
.........#
..........
..#.......
.......#..
..........
.#..^.....
........#.
#.........
......#...', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__6') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw,
            ROW_NUMBER() OVER () AS row_id
        FROM raw_data
    ),

    as_array AS (
        SELECT
            raw,
            row_id,
            STRING_SPLIT(raw, '') AS char_list

        FROM base
    ),

    as_chars AS (
        SELECT
            row_id,
            UNNEST(char_list) AS char_,
            GENERATE_SUBSCRIPTS(char_list, 1) AS col_id
        FROM as_array
    ),

    boundaries AS (
        SELECT
            MAX(row_id) AS max_row,
            MAX(col_id) AS max_col
        FROM as_chars
    ),

    starting_point AS (
        SELECT * FROM as_chars
        WHERE char_ = '^'
    ),

    blockers AS (
        SELECT * FROM as_chars
        WHERE char_ = '#'
    ),

    paths AS (
        SELECT
            1 AS dummy_row_id,
            2 AS dummy_col_id,
            row_id,
            col_id,
            [(row_id, col_id)] AS visited,
            1 AS iteration,
            'UP' AS direction,
            ['UP'] AS directions_taken
        FROM
            starting_point

        UNION ALL

        SELECT
            CASE
                WHEN paths.direction = 'UP' THEN MAX(blockers.row_id)
                WHEN paths.direction = 'RIGHT' THEN MIN(blockers.row_id)
                WHEN paths.direction = 'DOWN' THEN MIN(blockers.row_id)
                WHEN paths.direction = 'LEFT' THEN MIN(blockers.row_id)
            END AS true_row_id,
            CASE
                WHEN paths.direction = 'UP' THEN MIN(blockers.col_id)
                WHEN paths.direction = 'RIGHT' THEN MIN(blockers.col_id)
                WHEN paths.direction = 'DOWN' THEN MIN(blockers.col_id)
                WHEN paths.direction = 'LEFT' THEN MAX(blockers.col_id)
            END AS true_col_id,
            true_row_id
            + CASE
                WHEN paths.direction = 'UP' THEN +1 WHEN
                    paths.direction = 'DOWN'
                    THEN -1
                ELSE 0
            END AS new_row_id,
            true_col_id
            + CASE
                WHEN paths.direction = 'RIGHT' THEN -1 WHEN
                    paths.direction = 'LEFT'
                    THEN +1
                ELSE 0
            END AS new_col_id,
            LIST_APPEND(paths.visited, (new_row_id, new_col_id)) AS visited,
            paths.iteration + 1 AS iteration,
            CASE
                WHEN paths.direction = 'UP' THEN 'RIGHT'
                WHEN paths.direction = 'RIGHT' THEN 'DOWN'
                WHEN paths.direction = 'DOWN' THEN 'LEFT'
                WHEN paths.direction = 'LEFT' THEN 'UP'
            END AS new_direction,
            LIST_APPEND(paths.directions_taken, new_direction)
                AS directions_taken
        FROM paths
            LEFT JOIN blockers
                ON
                    -- row_id
                    (
                        CASE
                            WHEN paths.direction = 'RIGHT' THEN paths.row_id
                            WHEN paths.direction = 'LEFT' THEN paths.row_id
                        END = blockers.row_id
                        OR
                        CASE
                            WHEN paths.direction = 'UP' THEN paths.row_id
                        END > blockers.row_id
                        OR
                        CASE
                            WHEN paths.direction = 'DOWN' THEN paths.row_id
                        END < blockers.row_id
                    )
                    AND
                    -- col_id
                    (
                        CASE
                            WHEN paths.direction = 'UP' THEN paths.col_id
                            WHEN paths.direction = 'DOWN' THEN paths.col_id
                        END = blockers.col_id
                        OR
                        CASE
                            WHEN paths.direction = 'LEFT' THEN paths.col_id
                        END > blockers.col_id
                        OR
                        CASE
                            WHEN paths.direction = 'RIGHT' THEN paths.col_id
                        END < blockers.col_id
                    )

        WHERE
            TRUE -- iteration < 20
            AND blockers.col_id IS NOT NULL
        GROUP BY iteration, directions_taken, visited, paths.direction

    ),

    max_iteration AS (
        SELECT MAX(iteration) AS max_iteration
        FROM paths
    ),

    full_path AS (

        SELECT
            paths.*,
            CASE
                WHEN paths.direction = 'UP' THEN (0, paths.col_id)
                WHEN
                    paths.direction = 'DOWN'
                    THEN (boundaries.max_row, paths.col_id)
                WHEN paths.direction = 'LEFT' THEN (paths.row_id, 0)
                WHEN
                    paths.direction = 'RIGHT'
                    THEN (paths.row_id, boundaries.max_col)
            END AS next_position,
            LIST_APPEND(paths.visited, next_position) AS complete_visited
        FROM paths
            CROSS JOIN boundaries
        WHERE paths.iteration = (SELECT m.max_iteration FROM max_iteration AS m)

    ),

    turn_points AS (

        SELECT
            UNNEST(complete_visited)[1] AS row_id,
            UNNEST(complete_visited)[2] AS col_id,
            UNNEST(directions_taken) AS direction,
            GENERATE_SUBSCRIPTS(complete_visited, 1) AS point_order_id
        FROM full_path
    ),

    next_point AS (
        SELECT
            *,
            LEAD(row_id) OVER (ORDER BY point_order_id) AS next_row_id,
            LEAD(col_id) OVER (ORDER BY point_order_id) AS next_col_id
        FROM turn_points
    ),

    all_visited AS (
        SELECT
            next_point.point_order_id,
            as_chars.row_id,
            as_chars.col_id,
            next_point.direction
        FROM next_point
            LEFT JOIN as_chars
                ON
                    -- row_id
                    (
                        CASE
                            WHEN
                                next_point.direction IN ('LEFT', 'RIGHT')
                                THEN next_point.row_id
                        END
                        = as_chars.row_id

                        OR
                        (
                            CASE
                                WHEN
                                    next_point.direction = 'UP'
                                    THEN next_point.row_id
                            END
                            >= as_chars.row_id
                            AND next_point.next_row_id <= as_chars.row_id
                        )
                        OR
                        (
                            CASE
                                WHEN
                                    next_point.direction = 'DOWN'
                                    THEN next_point.row_id
                            END
                            <= as_chars.row_id
                            AND next_point.next_row_id >= as_chars.row_id
                        )

                    )
                    AND
                    -- col_id
                    (
                        CASE
                            WHEN
                                next_point.direction IN ('UP', 'DOWN')
                                THEN next_point.col_id
                        END
                        = as_chars.col_id

                        OR
                        (
                            CASE
                                WHEN
                                    next_point.direction = 'LEFT'
                                    THEN next_point.col_id
                            END
                            >= as_chars.col_id
                            AND next_point.next_col_id <= as_chars.col_id
                        )
                        OR
                        (
                            CASE
                                WHEN
                                    next_point.direction = 'RIGHT'
                                    THEN next_point.col_id
                            END
                            <= as_chars.col_id
                            AND next_point.next_col_id >= as_chars.col_id
                        )
                    )


    )

SELECT *
FROM all_visited

















--ben    1056 is too low
