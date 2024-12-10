{{
  config(
    materialized = 'table',
    )
}}
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

    starting_point AS (
        SELECT * FROM as_chars
        WHERE char_ = '^'
    ),

    blockers AS (
        SELECT * FROM as_chars
        WHERE char_ = '#'
    ),

    initial_path AS (
        SELECT DISTINCT
            row_id,
            col_id
        FROM {{ ref('day_06') }}
    ),

    initial_path_exclude_start AS (
        SELECT
            ip.*,
            ROW_NUMBER() OVER (ORDER BY ip.row_id, ip.col_id) AS block_iteration
        FROM initial_path AS ip
            LEFT JOIN
                starting_point
                ON
                    ip.row_id = starting_point.row_id
                    AND ip.col_id = starting_point.col_id
        WHERE starting_point.row_id IS NULL
    ),

    block_iterations AS (
        SELECT
            blockers.row_id,
            blockers.col_id,
            ip.block_iteration,
            'real' AS block_type
        FROM blockers
            CROSS JOIN initial_path_exclude_start AS ip
        UNION
        SELECT
            row_id,
            col_id,
            block_iteration,
            'introduced' AS block_type
        FROM initial_path_exclude_start
    ),

    paths AS (
        SELECT
            1 AS dummy_row_id,
            2 AS dummy_col_id,
            starting_point.row_id,
            starting_point.col_id,
            ip.block_iteration,
            'UP' AS direction,
            [(starting_point.row_id, starting_point.col_id, direction)]
                AS visited,
            1 AS iteration,
            ['UP'] AS directions_taken,
            FALSE AS is_loop
        FROM
            starting_point
            CROSS JOIN initial_path_exclude_start AS ip

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
            paths.block_iteration,
            CASE
                WHEN paths.direction = 'UP' THEN 'RIGHT'
                WHEN paths.direction = 'RIGHT' THEN 'DOWN'
                WHEN paths.direction = 'DOWN' THEN 'LEFT'
                WHEN paths.direction = 'LEFT' THEN 'UP'
            END AS new_direction,
            LIST_APPEND(
                paths.visited, (new_row_id, new_col_id, new_direction)
            ) AS visited,
            paths.iteration + 1 AS iteration,

            LIST_APPEND(paths.directions_taken, new_direction)
                AS directions_taken,
            (new_row_id, new_col_id, new_direction)
                IN paths.visited AS is_bool

        FROM paths
            LEFT JOIN block_iterations AS blockers
                ON
                    paths.block_iteration = blockers.block_iteration
                    AND
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
            AND NOT paths.is_loop
        GROUP BY
            iteration,
            directions_taken,
            visited,
            paths.direction,
            paths.block_iteration,
            paths.is_loop

    ),

    keep_loops_only AS (
        SELECT paths.*
        FROM paths
        WHERE paths.is_loop
    )

SELECT COUNT(DISTINCT block_iteration) AS part2 FROM keep_loops_only
