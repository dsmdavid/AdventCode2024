{# MMMSXXMASM
MSAMXMSMSA
AMXSXMAAMM
MSAMASMSMX
XMASAMXAMM
XXAMMXXAMA
SMSMSASXSS
SAXAMASAAA
MAMMMXMMMM
MXMXAXMASX #}

{# ..X...
.SAMX.
.A..A.
XMAS.S
.X.... #}

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
        SELECT unnest([-1, 0, 1]) AS modifier
    ),

    adjacent AS (
        SELECT
            r.modifier AS row_modifier,
            c.modifier AS col_modifier
        FROM next_pos AS r
            CROSS JOIN next_pos AS c
    ),

    as_words AS (
        WITH RECURSIVE
            word_creation AS (
                -- starting
                SELECT
                    'XMAS' AS target_word,
                    char_ AS current_word,
                    (row_id, col_id) AS current_position,
                    1 AS iteration,
                    [current_position] AS positions_visited,
                    row_id,
                    col_id,
                    adjacent.row_modifier,
                    adjacent.col_modifier
                FROM as_chars
                    CROSS JOIN adjacent

                WHERE
                    char_ = 'X'
                    AND NOT (row_modifier = 0 AND col_modifier = 0)

                UNION ALL

                SELECT
                    word_creation.target_word,
                    word_creation.current_word || char_ AS current_word,
                    (as_chars.row_id, as_chars.col_id) AS current_position,
                    word_creation.iteration + 1 AS iteration,
                    list_append(
                        word_creation.positions_visited,
                        (as_chars.row_id, as_chars.col_id)
                    ) AS positions_visited,
                    as_chars.row_id,
                    as_chars.col_id,
                    word_creation.row_modifier,
                    word_creation.col_modifier
                FROM word_creation
                    LEFT JOIN as_chars
                        ON (
                            (
                                word_creation.row_id
                                + word_creation.row_modifier
                                = as_chars.row_id
                            )
                            AND
                            (
                                word_creation.col_id
                                + word_creation.col_modifier
                                = as_chars.col_id
                            )
                        )
                        AND (
                            as_chars.char_
                            = word_creation.target_word[iteration + 1]
                        )

                WHERE
                    iteration <= 5
                    AND ((as_chars.row_id, as_chars.col_id) 
                    NOT IN word_creation.positions_visited)
                    AND word_creation.current_word || char_
                    = word_creation.target_word[1:iteration + 1]



            )

        SELECT * FROM word_creation
    )

SELECT count(DISTINCT positions_visited) FROM as_words
WHERE
    current_word = target_word
    AND len(positions_visited) = len(target_word)
