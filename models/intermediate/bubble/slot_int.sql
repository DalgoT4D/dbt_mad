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

    CASE
        WHEN "end_time_date" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("end_time_date", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS end_time,

    "slot_id_number"::text AS slot_id,
    "slot_name_text" AS slot_name,
    "removed_boolean" AS is_removed,
    
    CASE
        WHEN "start_time_date" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("start_time_date", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS start_time,

    "day_of_week_text" AS day_of_week,
    "reccuring_boolean" AS is_recurring,
    "academic_year_text" AS academic_year,
    "school_id_custom_partner"::text AS school_id,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('bubble_staging', 'slot') }}
WHERE "removed_boolean" IS NOT TRUE 