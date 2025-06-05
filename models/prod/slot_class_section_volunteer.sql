{{ config(materialized='table') }}

SELECT
    slot_class_section_volunteer_id AS slot_class_section_volunteer_id,
    slot_class_section_id AS slot_class_section_id,
    volunteer_id AS volunteer_id,
    academic_year AS academic_year,
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

FROM prod.slot_class_section_volunteer  