WITH
    raw_data AS (
        {% if var('day01', False) %}
        SELECT 
            regexp_split_to_table('3   4
4   3
2   5
1   3
3   9
3   3', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__1') }}
        {% endif %}
    ),

    split_selection AS (
        SELECT regexp_split_to_array(raw, ' ') AS split
        FROM raw_data
    ),

    list_one AS (
        SELECT split[1]::integer AS item
        FROM split_selection
    ),

    list_two AS (
        SELECT split[-1]::integer AS item
        FROM split_selection
    ),

    appearances_two AS (
        SELECT
            item,
            count(*) AS repetitions
        FROM list_two
        GROUP BY 1
    ),

    retrieve_repetitions AS (
        SELECT
            list_one.item,
            appearances_two.repetitions
        FROM list_one
            LEFT JOIN appearances_two
                ON list_one.item = appearances_two.item
    ),

    similarities AS (
        SELECT sum(item * repetitions) AS score
        FROM retrieve_repetitions

    )

SELECT score FROM similarities
