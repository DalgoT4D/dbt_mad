{{ config(materialized='view') }}

-- CO CRM Performance: Aggregated metrics per Community Organizer
-- Shows targets, partner assignments, and conversion stage distribution

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
    p.school_type,
    p.total_child_count AS partner_child_count,
    p.interested
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

co_partner_agreements AS (
  -- Combine CO data with latest agreements
  SELECT 
    pd.*,
    fa.current_status AS agreement_status,
    fa.conversion_stage,
    fa.potential_child_count,
    fa.updated_at AS agreement_last_updated
  FROM partner_details pd
  LEFT JOIN final_agreements fa 
    ON pd.partner_id = fa.partner_id
),

-- Get all unique conversion stages for pivoting
conversion_stages AS (
  SELECT DISTINCT conversion_stage
  FROM {{ ref('partner_agreements_int') }}
  WHERE conversion_stage IS NOT NULL
),

-- Aggregate metrics per CO
co_aggregated AS (
  SELECT 
    user_id,
    user_display_name,
    city,
    user_role,
    mou_target,
    child_count_target,
    school_leads_target,
    
    -- Partner assignment metrics
    COUNT(DISTINCT partner_id) AS total_partners_assigned,
    COUNT(DISTINCT CASE WHEN partner_id IS NOT NULL THEN partner_id END) AS active_partner_count,
    
    -- Child count metrics
    SUM(COALESCE(partner_child_count, 0)) AS total_partner_children,
    SUM(COALESCE(potential_child_count, 0)) AS total_potential_children,
    
    -- Agreement status counts
    COUNT(DISTINCT CASE WHEN agreement_status IS NOT NULL THEN partner_id END) AS partners_with_agreements,
    
    -- Conversion stage pivot - count partners in each stage
    COUNT(DISTINCT CASE WHEN conversion_stage = 'new' THEN partner_id END) AS stage_new_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'first_conversation' THEN partner_id END) AS stage_first_conversation_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'interested' THEN partner_id END) AS stage_interested_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'interested_but_facing_delay' THEN partner_id END) AS stage_interested_but_facing_delay_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'converted' THEN partner_id END) AS stage_converted_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'not_interested' THEN partner_id END) AS stage_not_interested_count,
    COUNT(DISTINCT CASE WHEN conversion_stage = 'dropped' THEN partner_id END) AS stage_dropped_count,
    
    -- Performance calculation
    CASE 
      WHEN mou_target > 0 THEN 
        ROUND(
          (COUNT(DISTINCT CASE WHEN conversion_stage = 'converted' THEN partner_id END)::NUMERIC / mou_target::NUMERIC) * 100, 
          2
        )
      ELSE 0 
    END AS mou_achievement_percentage,
    
    CASE 
      WHEN child_count_target > 0 THEN 
        ROUND(
          (SUM(COALESCE(potential_child_count, 0))::NUMERIC / child_count_target::NUMERIC) * 100, 
          2
        )
      ELSE 0 
    END AS child_count_achievement_percentage
    
  FROM co_partner_agreements
  GROUP BY 
    user_id,
    user_display_name,
    city,
    user_role,
    mou_target,
    child_count_target,
    school_leads_target
)

SELECT 
  user_id,
  user_display_name,
  city,
  user_role,
  mou_target,
  child_count_target,
  school_leads_target,
  total_partners_assigned,
  active_partner_count,
  total_partner_children,
  total_potential_children,
  partners_with_agreements,
  
  -- Conversion stage distribution
  stage_new_count,
  stage_first_conversation_count,
  stage_interested_count,
  stage_interested_but_facing_delay_count,
  stage_converted_count,
  stage_not_interested_count,
  stage_dropped_count,
  
  -- Performance metrics
  mou_achievement_percentage,
  child_count_achievement_percentage,
  
  -- Additional insights
  CASE 
    WHEN mou_achievement_percentage >= 100 THEN 'Target Met'
    WHEN mou_achievement_percentage >= 80 THEN 'On Track'
    WHEN mou_achievement_percentage >= 50 THEN 'Needs Attention'
    ELSE 'Critical'
  END AS performance_status

FROM co_aggregated
ORDER BY user_role, mou_achievement_percentage DESC