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
            SELECT raw FROM {{ ref('Day01_01_source') }}
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

    ordered_one AS (
        SELECT
            item,
            row_number() OVER (ORDER BY item) AS rn
        FROM list_one
    ),

    ordered_two AS (
        SELECT
            item,
            row_number() OVER (ORDER BY item) AS rn
        FROM list_two
    ),

    distances AS (
        SELECT
            ordered_one.item AS item_1,
            ordered_two.item AS item_2,
            abs(item_1 - item_2) AS distance
        FROM ordered_one
            LEFT JOIN ordered_two
                ON ordered_one.rn = ordered_two.rn
    )

SELECT sum(distance) FROM distances
