{{ config(materialized='table') }}

WITH nagpur_applicants AS (
    SELECT 
        application_id,
        applied_to_worknode_name,
        application_status,
        COUNT(*) as row_count
    FROM {{ ref('applicant_data_2025_int') }}
    WHERE applied_to_worknode_name = 'Nagpur'
      AND application_status IN ('APPLICATION_STATUS.PENDING', 'APPLICATION_STATUS.COMPLETED')
    GROUP BY application_id, applied_to_worknode_name, application_status
)

SELECT 
    'Total Distinct Applications' as metric,
    COUNT(DISTINCT application_id) as count
FROM nagpur_applicants

UNION ALL

SELECT 
    'Total Rows' as metric,
    SUM(row_count) as count
FROM nagpur_applicants

UNION ALL

SELECT 
    'Pending Applications' as metric,
    COUNT(DISTINCT CASE WHEN application_status = 'APPLICATION_STATUS.PENDING' THEN application_id END) as count
FROM nagpur_applicants

UNION ALL

SELECT 
    'Completed Applications' as metric,
    COUNT(DISTINCT CASE WHEN application_status = 'APPLICATION_STATUS.COMPLETED' THEN application_id END) as count
FROM nagpur_applicants

UNION ALL

SELECT 
    'Applications with Multiple Statuses' as metric,
    COUNT(DISTINCT application_id) as count
FROM nagpur_applicants
GROUP BY application_id
HAVING COUNT(DISTINCT application_status) > 1 