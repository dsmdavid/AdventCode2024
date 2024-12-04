WITH
    raw_data AS (
        {% if var('day04', False) %}
        SELECT 
            regexp_split_to_table('MMMSXXMASM
MSAMXMSMSA
AMXSXMAAMM
MSAMASMSMX
XMASAMXAMM
XXAMMXXAMA
SMSMSASXSS
SAXAMASAAA
MAMMMXMMMM
MXMXAXMASX
', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__4') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw,
            row_number() OVER () AS row_id
        FROM raw_data
    ),

    as_array AS (
        SELECT
            raw,
            row_id,
            string_split(raw, '') AS char_list

        FROM base
    ),

    as_chars AS (
        SELECT
            row_id,
            unnest(char_list) AS char_,
            generate_subscripts(char_list, 1) AS col_id
        FROM as_array
    ),

    next_pos AS (
        SELECT unnest([-1, 1]) AS modifier
    ),

    adjacent AS (
        SELECT
            r.modifier AS row_modifier,
            c.modifier AS col_modifier,
            'p'
            || cast(
                row_number()
                    OVER (ORDER BY row_modifier, col_modifier)
                AS varchar
            ) AS relative_position

        FROM next_pos AS r
            CROSS JOIN next_pos AS c
    ),

    starting AS (
        SELECT
            base.row_id,
            base.col_id,
            adj.row_modifier,
            adj.col_modifier,
            adj.relative_position

        FROM as_chars AS base
            CROSS JOIN adjacent AS adj
        WHERE base.char_ = 'A'
    ),

    as_words_chars AS (

        SELECT
            base.row_id,
            base.col_id,
            base.relative_position,
            as_chars.char_
        FROM starting AS base
            LEFT JOIN as_chars
                ON (
                    (base.row_id + row_modifier) = as_chars.row_id
                    AND
                    (base.col_id + col_modifier) = as_chars.col_id
                )
        WHERE as_chars.char_ IN ('M', 'S')
    ),

    pivoted_words AS (
        PIVOT as_words_chars
        ON relative_position
        USING max(char_)
    ),

    as_x_mas AS (
        SELECT
            row_id,
            col_id,
            CASE
                WHEN p1 = 'M' THEN p4 = 'S'
                WHEN p1 = 'S' THEN p4 = 'M'
                ELSE FALSE
            END
            AND
            CASE
                WHEN p2 = 'M' THEN p3 = 'S'
                WHEN p2 = 'S' THEN p3 = 'M'
                ELSE FALSE END
                AS valid_xmas
        FROM pivoted_words
        WHERE valid_xmas
    )

SELECT count(*) AS result FROM as_x_mas
