{{ config(materialized='table') }}

SELECT
    slot_id AS slot_id,
    slot_name AS slot_name,
    academic_year AS academic_year,
    day_of_week AS day_of_week,
    CASE
        WHEN start_time::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(start_time::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS start_time,
    CASE
        WHEN end_time::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(end_time::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS end_time,
    reccuring AS reccuring,
    school_id AS school_id,
    removed AS removed,
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

FROM prod.slot 