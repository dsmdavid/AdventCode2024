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
            -1 AS iteration,
            len(report_array) AS max_iterations,
            row_number() OVER () AS report_id

        FROM raw_data
    ),

    solver AS (
        WITH
        RECURSIVE
            recursive_check AS (
                -- start
                SELECT
                    report_array,
                    report_id,
                    original_report,
                    iteration,
                    max_iterations,
                    list_sort(report_array) == report_array AS increasing_list,
                    list_reverse_sort(report_array)
                    == report_array AS decreasing_list,
                    -- Any two adjacent levels differ by at least one and at most three.
                    list_reduce(
                        report_array,
                        (x, y) -> (
                            (
                                x <> 0
                                AND (1 <= abs(x - y))
                                AND (abs(x - y) <= 3)
                            )::integer
                            * y
                        )
                    )
                    <> 0 AS valid_rules,
                    greatest(increasing_list, decreasing_list)::integer
                    * valid_rules::integer AS valid_report
                FROM as_array

                UNION ALL

                SELECT
                    list_concat(
                        list_slice(
                            list_transform(
                                regexp_split_to_array(original_report, ' '),
                                x -> x::integer
                            ),
                            0, iteration
                        ),
                        list_slice(
                            list_transform(
                                regexp_split_to_array(original_report, ' '),
                                x -> x::integer
                            ),
                            iteration + 2, -1
                        )
                    ) AS report_array,
                    report_id,
                    original_report,
                    iteration + 1 AS iteration,
                    max_iterations,
                    list_sort(report_array)
                    == report_array AS increasing_list,
                    list_reverse_sort(report_array)
                    == report_array AS decreasing_list,
                    -- Any two adjacent levels differ by at least one and at most three.
                    list_reduce(
                        report_array,
                        (x, y) -> (
                            (
                                x <> 0
                                AND (1 <= abs(x - y))
                                AND (abs(x - y) <= 3)
                            )::integer
                            * y
                        )
                    )
                    <> 0 AS valid_rules,
                    greatest(increasing_list, decreasing_list)::integer
                    * valid_rules::integer AS valid_report
                FROM recursive_check
                WHERE
                --if we have found it's valid already, no need to go further
                    NOT recursive_check.valid_report
                    AND recursive_check.iteration <= max_iterations + 10
            )

        SELECT * FROM recursive_check
    )

SELECT sum(valid_report) FROM solver WHERE valid_report
