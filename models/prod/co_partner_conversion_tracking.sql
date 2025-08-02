{{ config(materialized='view') }}

-- CO CRM Performance: Individual records per Community Organizer and Partner
-- Shows targets, partner assignments, MOU data, and conversion stages
-- Simplified version without agreement status details

WITH co_base AS (
  -- Base CO data with calculated targets
  SELECT 
    user_id,
    user_display_name,
    city,
    user_role,
    
    -- Calculate targets based on role
    CASE 
      WHEN user_role = 'CO Part Time' THEN 1
      WHEN user_role = 'CO Full Time' THEN 5
    END AS mou_target,
    
    CASE 
      WHEN user_role = 'CO Part Time' THEN 50
      WHEN user_role = 'CO Full Time' THEN 250
    END AS child_count_target,
    
    CASE 
      WHEN user_role = 'CO Part Time' THEN 5  -- 5 * 1
      WHEN user_role = 'CO Full Time' THEN 25 -- 5 * 5
    END AS school_leads_target
    
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
    p.partner_name,
    p.school_type
  FROM co_partners cp
  LEFT JOIN {{ ref('partners_int') }} p 
    ON cp.partner_id = p.id
  WHERE p.removed = 'FALSE' 
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

-- Get latest conversion stage per partner (without agreement details)
latest_conversion_stage AS (
  SELECT 
    partner_id,
    conversion_stage,
    ROW_NUMBER() OVER (
      PARTITION BY partner_id 
      ORDER BY updated_at DESC
    ) as cs_rn
  FROM {{ ref('partner_agreements_int') }}
  WHERE conversion_stage IS NOT NULL
),

final_conversion_stage AS (
  SELECT partner_id, conversion_stage 
  FROM latest_conversion_stage 
  WHERE cs_rn = 1
),

co_partner_agreements AS (
  -- Combine CO data with MOU data and conversion stages
  SELECT 
    pd.*,
    lm.confirmed_child_count,
    lm.mou_status,
    lm.mou_sign_date,
    fcs.conversion_stage
  FROM partner_details pd
  LEFT JOIN latest_mou lm
    ON pd.partner_id = lm.partner_id
  LEFT JOIN final_conversion_stage fcs
    ON pd.partner_id = fcs.partner_id
),

conversion_stages AS (
  SELECT DISTINCT conversion_stage
  FROM {{ ref('partner_agreements_int') }}
  WHERE conversion_stage IS NOT NULL
)

SELECT 
  user_id,
  user_display_name,
  city,
  user_role,
  mou_target,
  child_count_target,
  school_leads_target,
  partner_id,
  partner_name,
  school_type,
  assignment_date,
  confirmed_child_count,
  mou_status,
  mou_sign_date,
  conversion_stage AS original_conversion_stage,

  -- Modified conversion stage grouping
  CASE 
    WHEN conversion_stage = 'new' THEN 'new'
    WHEN conversion_stage = 'first_conversation' THEN 'first_conversation'
    WHEN conversion_stage IN ('interested', 'interested_but_facing_delay') THEN 'interested'
    WHEN conversion_stage = 'converted' THEN 'converted'
    WHEN conversion_stage IN ('not_interested', 'dropped') THEN 'lost'
    ELSE conversion_stage
  END AS simplified_conversion_stage

FROM co_partner_agreements
ORDER BY user_role, user_display_name, partner_name