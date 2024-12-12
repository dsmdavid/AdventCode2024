{% set input_1 = '''..90..9
...1.98
...2..7
6543456
765.987
876....
987....''' %}
{% set input_2 = '''89010123
78121874
87430965
96549874
45678903
32019012
01329801
10456732''' %}


WITH RECURSIVE
    raw_data AS (
        {% if var('day11', False) %}
        SELECT 
            regexp_split_to_table('{{ input_1 }}', '\n') AS raw
        {% else %}
            SELECT
                REPLACE(raw, '#', '')
                    AS raw
            FROM {{ ref('2024__11') }}
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
            GENERATE_SUBSCRIPTS(char_list, 1) AS col_id,
            TRY_CAST(char_ AS integer) AS int_
        FROM as_array
    ),

    steps AS (
        SELECT
            1 AS row_mod,
            0 AS col_mod
        UNION ALL
        SELECT
            -1 AS row_mod,
            0 AS col_mod
        UNION ALL
        SELECT
            0 AS row_mod,
            1 AS col_mod
        UNION ALL
        SELECT
            0 AS row_mod,
            -1 AS col_mod

    ),

    starting_points AS (
        SELECT * FROM as_chars
        WHERE char_ = '0'
    ),

    paths AS (
        SELECT
            row_id AS starting_point_row_id,
            col_id AS starting_point_col_id,
            row_id AS current_row_id,
            col_id AS current_col_id,
            [(row_id, col_id)] AS visited,
            char_ AS current_char,
            int_ AS current_number,
            FALSE AS is_peak
        FROM starting_points

        UNION ALL

        SELECT
            paths.starting_point_row_id,
            paths.starting_point_col_id,
            as_chars.row_id AS current_row_id,
            as_chars.col_id AS current_col_id,
            LIST_APPEND(
                paths.visited,
                (as_chars.row_id, as_chars.col_id)
            ) AS new_visited,
            as_chars.char_,
            as_chars.int_,
            as_chars.char_ = 9 AS is_peak

        FROM paths
            CROSS JOIN steps
            LEFT JOIN as_chars
                ON
                    paths.current_row_id + steps.row_mod = as_chars.row_id
                    AND
                    paths.current_col_id + steps.col_mod = as_chars.col_id
                    AND paths.current_number + 1 = as_chars.int_
        WHERE
            NOT paths.is_peak
            AND as_chars.row_id IS NOT NULL

    ),

    reached_peaks AS (
        SELECT *
        FROM paths
        WHERE is_peak
    )

SELECT
    COUNT(
        DISTINCT CAST(starting_point_row_id AS varchar)
        || '|'
        || CAST(starting_point_col_id AS varchar)
        || CAST(current_row_id AS varchar)
        || '|'
        || CAST(current_col_id AS varchar)
    ) AS result,
    'part1' AS part_
FROM reached_peaks

UNION ALL

SELECT
    COUNT(DISTINCT visited) AS result,
    'part2' AS part_
FROM reached_peaks
