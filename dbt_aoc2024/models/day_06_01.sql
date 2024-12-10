SELECT
    COUNT(DISTINCT CAST(row_id AS varchar) || '-' || CAST(col_id AS varchar))
        AS part1
FROM {{ ref('day_06') }}
