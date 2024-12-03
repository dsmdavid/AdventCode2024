WITH
    raw_data AS (
        {% if var('day03', False) %}
        SELECT 
            regexp_split_to_table($$xmul(2,4)&mul[3,7]!^don't()_mul(5,5)+mul(32,64](mul(11,8)undo()?mul(8,5))$$, '\n') AS raw -- '
        {% else %}
            SELECT raw FROM {{ ref('2024__3') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw,
            row_number() OVER () AS raw_id
        FROM raw_data
    ),

    replaced_tokens AS (
        SELECT
            raw,
            raw_id,
            regexp_replace(
                replace(replace(raw, 'do()', 'DVDON'), 'don''t()', 'DVDOF'),
                '^(.)',
                'DVDON\1' -- adds
            ) AS replaced,
            regexp_split_to_table(replaced, '(DVD)') AS tokenized,
            tokenized[0:2] == 'ON' AS valid_inputs,
            generate_subscripts(regexp_split_to_array(replaced, '(DVD)'), 1)
                AS index_

        FROM base
    ),

    add_id AS (
        SELECT
            *,
            CASE
                WHEN index_ == 2 AND raw_id == 1 THEN valid_inputs
                WHEN
                    index_ == 2
                    THEN lag(valid_inputs) OVER (ORDER BY raw_id, index_)
                ELSE valid_inputs
            END AS fixed_valid_inputs
        FROM replaced_tokens
        WHERE index_ > 1 -- remove empty start for window function above to be correct
        ORDER BY raw_id, index_
    ),

    as_array AS (
        SELECT
            raw,
            tokenized,
            raw_id,
            index_,
            unnest(
                regexp_extract_all(tokenized, 'mul\(\d{1,3},\d{1,3}\)')
            ) AS valid_multiplications,
            list_transform(
                regexp_extract_all(valid_multiplications, '\d{1,3}'),
                x -> x::integer
            ) AS numbers,
            list_reduce(
                numbers,
                (x, y) -> x * y
            ) AS multiplication_result
        FROM add_id
        WHERE fixed_valid_inputs
    )

SELECT sum(multiplication_result) as result
FROM as_array
