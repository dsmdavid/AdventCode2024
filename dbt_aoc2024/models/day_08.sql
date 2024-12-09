{% set input_1 = '''............
........0...
.....0......
.......0....
....0.......
......A.....
............
............
........A...
.........A..
............
............''' %}
{% set input_2 = '''..........
..........
..........
....a.....
........a.
.....a....
..........
..........
..........
..........''' %}


WITH
    raw_data AS (
        {% if var('day08', False) %}
        SELECT 
            regexp_split_to_table('{{ input_1 }}', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__8') }}
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

    boundaries AS (
        SELECT
            max(row_id) AS max_row_id,
            max(col_id) AS max_col_id
        FROM as_chars
    ),

    matched_antennas AS (
        SELECT
            m1.row_id AS m1_row_id,
            m1.col_id AS m1_col_id,
            m2.row_id AS m2_row_id,
            m2.col_id AS m2_col_id,
            m1.char_,
            abs(m1.row_id - m2.row_id) AS distance_row,
            abs(m1.col_id - m2.col_id) AS distance_col
        FROM as_chars AS m1
            LEFT JOIN as_chars AS m2
                ON
                    m1.char_ = m2.char_
                    -- same antenna
                    AND NOT (m1.row_id = m2.row_id AND m1.col_id = m2.col_id)
        WHERE m1.char_ <> '.'
    ),

    antinodes AS (
        SELECT
            m1_row_id,
            m1_col_id,
            m2_row_id,
            m2_col_id,
            char_,
            distance_row,
            distance_col,
            CASE
                WHEN m1_row_id > m2_row_id THEN m1_row_id + distance_row
                WHEN m1_row_id < m2_row_id THEN m1_row_id - distance_row
                ELSE m1_row_id
            END AS new_row_id,
            CASE
                WHEN m1_col_id > m2_col_id THEN m1_col_id + distance_col
                WHEN m1_col_id < m2_col_id THEN m1_col_id - distance_col
                ELSE m1_col_id
            END AS new_col_id
        FROM matched_antennas
    )
    {#
    -- for debug output grid printing 
    distinct_antinodes AS (
        SELECT DISTINCT
            new_row_id,
            new_col_id
        FROM antinodes
    ),

    filled AS (

        SELECT
            as_chars.row_id,
            as_chars.col_id,
            coalesce(
                CASE
                    WHEN as_chars.char_ = '.' THEN NULL
                    ELSE as_chars.char_
                END,
                CASE
                    WHEN distinct_antinodes.new_row_id IS NOT NULL THEN '#'
                END,
                '.'
            ) AS print_char
        FROM as_chars
            LEFT JOIN distinct_antinodes
                ON
                    as_chars.row_id = distinct_antinodes.new_row_id
                    AND as_chars.col_id = distinct_antinodes.new_col_id
    ),

     grouped_array AS (
        SELECT
            filled.row_id,
            listagg(filled.print_char, '' ORDER BY filled.col_id) AS chars
        FROM filled
        GROUP BY filled.row_id
    )
    
    select * from grouped_array
    order by row_id #}

SELECT
    count(
        DISTINCT cast(antinodes.new_row_id AS varchar)
        || '|'
        || cast(antinodes.new_col_id AS varchar)
    ) AS part1
FROM antinodes
    CROSS JOIN boundaries
WHERE
    antinodes.new_row_id > 0
    AND antinodes.new_row_id <= boundaries.max_row_id
    AND antinodes.new_col_id > 0
    AND antinodes.new_col_id <= boundaries.max_col_id
