{{ config(materialized='table') }}

SELECT
    class_section_id AS class_section_id,
    academic_year AS academic_year,
    section_name AS section_name,
    removed AS removed,
    is_active AS is_active,
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

FROM prod.class_section 