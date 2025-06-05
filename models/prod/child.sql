{{ config(materialized='table') }}

SELECT
    child_id AS child_id,
    first_name AS first_name,
    last_name AS last_name,
    gender AS gender,
    CASE
        WHEN dob::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(dob::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS dob,
    city AS city,
    CASE
        WHEN mad_joining_date::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(mad_joining_date::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS mad_joining_date,
    CASE
        WHEN date_of_enrollment::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(date_of_enrollment::text, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS date_of_enrollment,
    mother_tounge AS mother_tounge,
    age AS age,
    removed AS removed,
    class_id AS class_id,
    school_class_id AS school_class_id,
    school_id AS school_id,
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

FROM prod.child 