{{ config(materialized='table') }}

SELECT 
    CASE 
        WHEN city IS NOT NULL THEN city
        ELSE 'unmapped'
    END AS city,
    school,
    COUNT(DISTINCT student_id) AS total_children_count
FROM {{ ref('school_city_mapping') }}
GROUP BY 
    CASE 
        WHEN city IS NOT NULL THEN city
        ELSE 'unmapped'
    END,
    school
ORDER BY 
    CASE 
        WHEN city IS NOT NULL THEN city
        ELSE 'unmapped'
    END,
    total_children_count DESC 