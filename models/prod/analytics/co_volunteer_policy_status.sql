{{ config(materialized='view') }}

WITH e2_volunteers AS (
    SELECT 
        user_id,
        selected_for_work_node_name,
        code_of_conduct_policy_accepted,
        child_protection_policy_accepted
    FROM {{ ref('applicant_data_2025_int') }}
    WHERE selected_for_work_node_name = 'E2'
      AND user_id IS NOT NULL
),

all_cos AS (
    SELECT 
        user_id,
        user_display_name AS co_name,
        city,
        user_role
    FROM {{ ref('user_data_int') }}
    WHERE user_role IN ('CO Part Time', 'CO Full Time')
),

volunteer_details AS (
    SELECT 
        v.user_id::text AS user_id,
        v.code_of_conduct_policy_accepted,
        v.child_protection_policy_accepted,
        u.reporting_manager_user_id,
        u.user_display_name AS volunteer_name
    FROM e2_volunteers v
    JOIN {{ ref('user_data_int') }} u
        ON v.user_id::text = u.user_id
)

SELECT 
    co.city,
    co.co_name,
    vd.volunteer_name,
    CASE 
        WHEN vd.code_of_conduct_policy_accepted = 'true' THEN 'Yes'
        ELSE 'No'
    END AS coc_accepted,
    CASE 
        WHEN vd.child_protection_policy_accepted = 'true' THEN 'Yes'
        ELSE 'No'
    END AS cpp_accepted
FROM all_cos co
LEFT JOIN volunteer_details vd
    ON co.user_id::float = vd.reporting_manager_user_id
ORDER BY co.city, co.co_name, vd.volunteer_name 