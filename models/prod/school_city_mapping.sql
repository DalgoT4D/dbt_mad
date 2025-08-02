{{ config(materialized='table') }}

WITH distinct_school_city AS (
    SELECT DISTINCT
        center AS school,
        city
    FROM {{ ref('class_ops_master_data_int') }}
    WHERE center IS NOT NULL
      AND city IS NOT NULL
)

SELECT 
    s.student_id,
    s.student_name,
    s.school,
    s.location,
    s.school_id,
    s.active_status,
    s.medium_of_instruction,
    s.class,
    s.course,
    s.gender,
    s.school_group,
    s.student_role_no,
    dsc.city
FROM {{ ref('students_data_int') }} s
LEFT JOIN distinct_school_city dsc
    ON s.school = dsc.school
WHERE s.active_status = 'Active' 