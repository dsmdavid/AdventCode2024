WITH
    raw_data AS (
        {% if var('day03', False) %}
        SELECT 
            regexp_split_to_table('xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__3') }}
        {% endif %}
    ),

    as_array AS (
        SELECT
            raw,
            unnest(
                regexp_extract_all(raw, 'mul\(\d{1,3},\d{1,3}\)')
            ) AS valid_multiplications,
            list_transform(
                regexp_extract_all(valid_multiplications, '\d{1,3}'),
                x -> x::integer
            ) AS numbers,
            list_reduce(
                numbers,
                (x, y) -> x * y
            ) AS multiplication_result
        FROM raw_data
    )

SELECT sum(multiplication_result) FROM as_array
