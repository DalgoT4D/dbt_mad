{{ config(materialized='table') }}

SELECT
    "_id" AS id,
    "Created_By" AS created_by,
    "co_id_user" AS co_id,
    
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
        WHEN "end_date_date" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE("end_date_date", 'YYYY-MM-DD')
        ELSE NULL
    END AS end_date,

    "removed_boolean" AS is_removed,

    CASE
        WHEN "start_date_date" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE("start_date_date", 'YYYY-MM-DD')
        ELSE NULL
    END AS start_date,

    "is_active_boolean" AS is_active,
    "academic_year_text" AS academic_year,
    "co_chapter_id_number" AS co_chapter_id,
    "chapter_id_custom_chapter" AS chapter_id,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('bubble_staging', 'co_chapter') }}
WHERE "removed_boolean" IS NOT TRUE 