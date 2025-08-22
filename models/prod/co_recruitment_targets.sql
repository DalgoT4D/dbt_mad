{{ config(
  materialized='table',
  tags=["co_recruitment", "analytics"]
) }}

WITH cte1 AS (
  -- Get CO Part Time and Full Time users
  SELECT 
    user_id,
    user_display_name as user_name
  FROM {{ ref('user_data_int') }}
  WHERE user_role IN ('CO Part Time', 'CO Full Time')
),

cte2 AS (
  -- Get latest MOU per partner with partner and CO information
  SELECT DISTINCT
    m.partner_id,
    m.confirmed_child_count,
    p.partner_name,
    pc.co_id
  FROM {{ ref('mous_int') }} m
  LEFT JOIN (
    SELECT DISTINCT id, partner_name 
    FROM {{ ref('partners_int') }}
  ) p ON m.partner_id = p.id
  LEFT JOIN {{ ref('partner_cos_int') }} pc ON m.partner_id = pc.partner_id
  WHERE m.updated_at = (
    SELECT MAX(updated_at) 
    FROM {{ ref('mous_int') }} m2 
    WHERE m2.partner_id = m.partner_id
  )
),

cte3 AS (
  -- Join CO users with partner data and calculate recruitment targets
  SELECT 
    c1.user_id,
    c1.user_name,
    c2.partner_id,
    c2.partner_name,
    c2.confirmed_child_count,
    c2.co_id,
    ROUND((c2.confirmed_child_count * 4.0 / 5.0), 2) AS recruitment_target
  FROM cte1 c1
  LEFT JOIN cte2 c2 ON c1.user_id = c2.co_id
)

SELECT * FROM cte3
