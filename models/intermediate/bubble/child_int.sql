{{ config(materialized='table') }}

SELECT
    "_id" AS id,
    "Created_By" AS created_by,
    
    CASE
        WHEN "dob_date" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE("dob_date", 'YYYY-MM-DD')
        ELSE NULL
    END AS date_of_birth,

    "city_text" AS city,
    "age_number" AS age,
    "gender_text" AS gender,
    
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

    "last_name_text" AS last_name,
    "child_id_number" AS child_id,
    "first_name_text" AS first_name,
    "removed_boolean" AS is_removed,
    "is_active_boolean" AS is_active,
    "mother_tounge_text" AS mother_tongue,
    "class_id_custom_class" AS class_id,
    
    CASE
        WHEN "mad_joining_date_date" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE("mad_joining_date_date", 'YYYY-MM-DD')
        ELSE NULL
    END AS mad_joining_date,
    
    CASE
        WHEN "date_of_enrollment_date" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE("date_of_enrollment_date", 'YYYY-MM-DD')
        ELSE NULL
    END AS date_of_enrollment,

    "school_id_custom_partner" AS school_id,
    "school_class_id_custom_school_class" AS school_class_id,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('bubble_staging', 'child') }}
WHERE "removed_boolean" IS NOT TRUE 