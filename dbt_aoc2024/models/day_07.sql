{{
  config(
    materialized = 'table',
    )
}}
/*
Getting some error if I try to run the whole script as a single object or as a view. 
Materializating this first part as a table solves the issue, no idea what's happening.
*/
{% set input_1 = '''190: 10 19
3267: 81 40 27
83: 17 5
156: 15 6
7290: 6 8 6 15
161011: 16 10 13
192: 17 8 14
21037: 9 7 18 13
292: 11 6 16 20''' %}

WITH
    raw_data AS (
        {% if var('day07', False) %}
        SELECT 
            regexp_split_to_table('{{ input_1 }}', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__7') }}
        {% endif %}
    ),

    base AS (
        SELECT
            raw,
            row_number() OVER () AS row_id
        FROM raw_data
        WHERE raw <> ''
    ),

    as_array AS (
        SELECT
            raw,
            row_id,
            cast(split(raw, ':')[1] AS uhugeint) AS test_number,
            list_transform(string_split(split(raw, ':')[2], ' '), x -> x)
                AS number_list

        FROM base
    ),

    as_numbers_prep1 AS (
        SELECT
            row_id,
            test_number,
            len(number_list) AS n_digits,
            unnest(number_list) AS number_operands,
            generate_subscripts(number_list, 1) AS col_id
        FROM as_array

    ),

    as_numbers AS (
        SELECT
            as_numbers_prep1.row_id,
            as_numbers_prep1.test_number,
            cast(number_operands AS uhugeint) AS number_operands,
            as_numbers_prep1.col_id,
            len(number_operands) AS n_digits
        FROM as_numbers_prep1
        WHERE as_numbers_prep1.number_operands <> ''
    )

SELECT * FROM as_numbers
