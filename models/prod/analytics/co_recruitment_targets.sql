{{ config(materialized='table') }}
-- CO CRM Performance: Aggregated metrics per Community Organizer
-- Shows targets, partner assignments, and conversion stage distribution
WITH co_base AS (
  -- Base CO data
  SELECT
    user_id,
    user_display_name,
    city,
    user_role
  FROM {{ ref('user_data_int') }}
  WHERE user_role IN ('CO Part Time', 'CO Full Time')
),
co_partners AS (
  -- Join with partner assignments
  SELECT
    cb.*,
    pc.partner_id,
    pc.created_at AS assignment_date
  FROM co_base cb
  LEFT JOIN {{ ref('partner_cos_int') }} pc
    ON cb.user_id = pc.co_id
),
partner_details AS (
  -- Add partner information
  SELECT
    cp.*,
    p.partner_name
  FROM co_partners cp
  LEFT JOIN {{ ref('partners_int') }} p
    ON cp.partner_id = p.id
  Where p.removed = 'FALSE'
),
latest_agreements AS (
  -- Get latest agreement per partner
  SELECT
    partner_id,
    current_status,
    conversion_stage,
    potential_child_count,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY partner_id
      ORDER BY updated_at DESC
    ) as rn
  FROM {{ ref('partner_agreements_int') }}
),
final_agreements AS (
  SELECT * FROM latest_agreements WHERE rn = 1
),
mou_data AS (
  -- Get MOU data for confirmed child counts
  SELECT
    partner_id,
    confirmed_child_count,
    mou_status,
    mou_sign_date,
    ROW_NUMBER() OVER (
      PARTITION BY partner_id
      ORDER BY updated_at DESC
    ) as mou_rn
  FROM {{ ref('mous_int') }}
),
latest_mou AS (
  SELECT * FROM mou_data WHERE mou_rn = 1
),
co_partner_agreements AS (
  -- Combine CO data with latest agreements and MOU data
  SELECT
    pd.*,
    fa.current_status AS agreement_status,
    fa.conversion_stage,
    fa.updated_at AS agreement_last_updated,
    lm.confirmed_child_count,
    lm.mou_status,
    lm.mou_sign_date
  FROM partner_details pd
  LEFT JOIN final_agreements fa
    ON pd.partner_id = fa.partner_id
  LEFT JOIN latest_mou lm
    ON pd.partner_id = lm.partner_id
  WHERE fa.conversion_stage = 'converted'
),
school_volunteer_counts AS (
  -- Aggregate volunteer counts per school
  SELECT
    sv.school_id,
    COUNT(DISTINCT sv.volunteer_id) AS recruited_volunteer_count
  FROM {{ ref('school_volunteer_int') }} sv
  WHERE sv.removed = 'FALSE'
  GROUP BY sv.school_id
),
crm_bubble_mapping AS (
  -- Map CRM partner IDs to Bubble partner IDs using name matching
  -- This handles the fact that CRM and Bubble use different ID systems
  SELECT DISTINCT
    p.id AS crm_partner_id,
    p.partner_name,
    bp.partner_id1 AS bubble_partner_id
  FROM {{ ref('partners_int') }} p
  INNER JOIN (
    SELECT DISTINCT ON (partner_name) 
      partner_name, 
      partner_id1
    FROM {{ ref('partner_int') }}
    ORDER BY partner_name, partner_id1
  ) bp ON p.partner_name = bp.partner_name
  WHERE p.removed = 'FALSE' 
    AND bp.partner_name IS NOT NULL
    AND p.partner_name IS NOT NULL
)
SELECT
  cpa.user_id,
  cpa.user_display_name as "CO Name",
  cpa.city as "City",
  cpa.user_role as "Role",
  cpa.partner_id,
  cpa.partner_name as "Partner Name",
  cpa.confirmed_child_count,
  CEIL((4.0/5.0) * COALESCE(cpa.confirmed_child_count, 0)) AS volunteer_recruitment_target,
  COALESCE(svc.recruited_volunteer_count, 0) AS recruited_volunteer_count,
  CASE 
    WHEN CEIL((4.0/5.0) * COALESCE(cpa.confirmed_child_count, 0)) > 0 
    THEN ROUND((COALESCE(svc.recruited_volunteer_count, 0) * 100.0) / CEIL((4.0/5.0) * COALESCE(cpa.confirmed_child_count, 0)), 2)
    ELSE 0 
  END AS recruitment_percentage
FROM co_partner_agreements cpa
LEFT JOIN crm_bubble_mapping cbm ON cpa.partner_id = cbm.crm_partner_id
LEFT JOIN school_volunteer_counts svc ON cbm.bubble_partner_id = svc.school_id
ORDER BY cpa.user_role, cpa.user_display_name