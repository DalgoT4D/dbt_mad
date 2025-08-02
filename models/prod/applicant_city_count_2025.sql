{{ config(materialized='table') }}

WITH distinct_applications AS (
    SELECT DISTINCT
        application_id,
        applied_to_worknode_name,
        application_status
    FROM {{ ref('applicant_data_2025_int') }}
    WHERE application_status IN ('APPLICATION_STATUS.PENDING', 'APPLICATION_STATUS.COMPLETED')
      AND applied_to_worknode_name IS NOT NULL
)

SELECT 
    CASE 
        WHEN applied_to_worknode_name IS NOT NULL THEN applied_to_worknode_name
        ELSE 'unmapped'
    END AS worknode_name,
    COUNT(DISTINCT application_id) AS total_applicants_count,
    COUNT(DISTINCT CASE WHEN application_status = 'APPLICATION_STATUS.PENDING' THEN application_id END) AS pending_applicants_count,
    COUNT(DISTINCT CASE WHEN application_status = 'APPLICATION_STATUS.COMPLETED' THEN application_id END) AS completed_applicants_count
FROM distinct_applications
GROUP BY CASE 
    WHEN applied_to_worknode_name IS NOT NULL THEN applied_to_worknode_name
    ELSE 'unmapped'
END
ORDER BY total_applicants_count DESC 