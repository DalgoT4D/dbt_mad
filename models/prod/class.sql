{{ config(materialized='table') }}

SELECT
    class_id AS class_id,
    class_name AS class_name,
    program_id AS program_id,
    CASE
        WHEN created_date::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(created_date::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS created_date,
    CASE
        WHEN modified_date::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(modified_date::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS modified_date

FROM prod.class 