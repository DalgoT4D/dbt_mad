{{ config(materialized='table') }}

SELECT
    "_id" AS id,
    "Created_By" AS created_by,
    
    CASE
        WHEN "Created_Date" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("Created_Date", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS created_date,

    CASE
        WHEN "Modified_Date" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("Modified_Date", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS modified_date,

    "removed_boolean" AS is_removed,
    "volunteer_id_user"::text AS volunteer_id,
    "academic_year_text" AS academic_year,
    "slot_class_section_volunteer_id_number"::text AS slot_class_section_volunteer_id,
    "slot_class_section_id_custom_volunteer_class_section"::text AS slot_class_section_id,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('bubble_staging', 'slot_class_section_volunteer') }}
WHERE "removed_boolean" IS NOT TRUE 