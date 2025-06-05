{{ config(materialized='table') }}

SELECT
    slot_class_section_id AS slot_class_section_id,
    slot_id AS slot_id,
    class_section_id AS class_section_id,
    class_section_subject_id AS class_section_subject_id,
    academic_year AS academic_year,
    removed AS removed,
    is_active AS is_active,
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

FROM prod.slot_class_section 