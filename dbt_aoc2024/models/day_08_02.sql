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
{% set input_3 ='''T.........
...T......
.T........
..........
..........
..........
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

    backbone AS (
        SELECT
            backbone.row_id,
            backbone.col_id,
            backbone.char_,
            abs(backbone.row_id - matched_antennas.m1_row_id)
            / matched_antennas.distance_row AS row_d1,
            abs(backbone.row_id - matched_antennas.m2_row_id)
            / matched_antennas.distance_row AS row_d2,
            abs(backbone.col_id - matched_antennas.m1_col_id)
            / matched_antennas.distance_col AS col_d1,
            abs(backbone.col_id - matched_antennas.m2_col_id)
            / matched_antennas.distance_col AS col_d2,
            row_d1 = col_d1 AND row_d2 = col_d2
                AS is_multiple

        FROM as_chars AS backbone
            CROSS JOIN matched_antennas
    ),

    distinct_antinodes AS (
        SELECT
            row_id,
            col_id,
            char_,
            CASE WHEN max(is_multiple) = 1 THEN '#' ELSE char_ END AS print_char
        FROM backbone
        GROUP BY 1, 2, 3
    )

    {# /* for debug - printing grid */
    , grouped_array AS (
        SELECT
            row_id,
            listagg(print_char, '' ORDER BY col_id) AS chars
        FROM distinct_antinodes
        GROUP BY row_id
    )
    
    select * from grouped_array
    order by row_id 
    #}


SELECT count(DISTINCT cast(row_id AS varchar) || '|' || cast(col_id AS varchar)) AS part2
FROM distinct_antinodes
WHERE print_char = '#'
