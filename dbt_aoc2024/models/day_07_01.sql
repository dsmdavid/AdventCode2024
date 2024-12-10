WITH RECURSIVE
    as_numbers AS (SELECT * FROM {{ ref('day_07') }}
    ),

    ordered_equations AS (
        SELECT
            row_id,
            test_number,
            number_operands,
            row_number()
                OVER (PARTITION BY row_id ORDER BY col_id ASC)
                AS number_order
        FROM as_numbers
        WHERE number_operands IS NOT NULL
        ORDER BY row_id, number_order
    ),

    operations AS (
        SELECT '*' AS operation_
        UNION ALL
        SELECT '+' AS operation_
    ),

    max_number_order AS (
        SELECT
            row_id,
            max(number_order) AS max_number_order
        FROM ordered_equations
        GROUP BY row_id
    ),

    solver AS (

        SELECT
            ordered_equations.row_id,
            ordered_equations.test_number,
            ordered_equations.number_operands,
            ordered_equations.number_order,
            1 AS depth_,
            max_number_order.max_number_order,
            FALSE AS solved
        FROM ordered_equations
            LEFT JOIN
                max_number_order
                ON ordered_equations.row_id = max_number_order.row_id
        WHERE ordered_equations.number_order = 1

        UNION ALL

        SELECT
            s.row_id,
            s.test_number,
            CASE
                WHEN
                    operations.operation_ = '*'
                    THEN s.number_operands * e.number_operands
                WHEN
                    operations.operation_ = '+'
                    THEN s.number_operands + e.number_operands
            END AS number_operands,
            s.number_order,
            s.depth_ + 1 AS depth_,
            s.max_number_order,
            (s.test_number = CASE
                WHEN
                    operations.operation_ = '*'
                    THEN s.number_operands * e.number_operands
                WHEN
                    operations.operation_ = '+'
                    THEN s.number_operands + e.number_operands
            END) AND (e.number_order = s.max_number_order)
                AS solved_

        FROM solver AS s
            CROSS JOIN operations
            LEFT JOIN ordered_equations AS e
                ON
                    s.row_id = e.row_id
                    AND s.number_order + s.depth_ = e.number_order
        WHERE
            e.row_id IS NOT NULL
            AND NOT s.solved
    -- and s.number_operands * e.number_operands <= s.test_number 
    ),

    solved AS (
        SELECT DISTINCT
            row_id,
            test_number
        FROM solver
        WHERE solved
    )

SELECT sum(test_number) AS part1 FROM solved
