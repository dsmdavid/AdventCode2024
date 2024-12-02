WITH
    raw_data AS (
        {% if var('day02', False) %}
        SELECT 
            regexp_split_to_table('7 6 4 2 1
1 2 7 8 9
9 7 6 2 1
1 3 2 4 5
8 6 4 4 1
1 3 6 7 9', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__2') }}
        {% endif %}
    ),

    as_array AS (
        SELECT
            raw AS original_report,
            list_transform(regexp_split_to_array(raw, ' '), x -> x::integer)
                AS report_array,
            row_number() OVER () AS report_id,
            list_sort(report_array) == report_array AS increasing_list,
            -- Any two adjacent levels differ by at least one and at most three.
            list_reverse_sort(report_array) == report_array AS decreasing_list,
            list_reduce(
                report_array,
                (x, y) -> (
                    (
                        x <> 0 AND (1 <= abs(x - y)) AND (abs(x - y) <= 3)
                    )::integer
                    * y
                )
            )
            <> 0 AS valid_rules,
            greatest(increasing_list, decreasing_list)::integer
            * valid_rules::integer AS valid_report
        FROM raw_data
    )

SELECT sum(valid_report) FROM as_array
