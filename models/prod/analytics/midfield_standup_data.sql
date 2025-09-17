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
    mou_start_date,
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
    lm.mou_sign_date,
    lm.mou_start_date
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
),
slot_volunteer_counts AS (
  -- Aggregate volunteer counts per school_id through slot_class_section
  SELECT
    csi.school_id,
    COUNT(DISTINCT scsv.volunteer_id) AS assigned_volunteer_count
  FROM {{ ref('slot_class_section_volunteer_int') }} scsv
  INNER JOIN {{ ref('slot_class_section_int') }} scs
    ON scsv.slot_class_section_id = scs.slot_class_section_id
  INNER JOIN {{ ref('class_section_int') }} csi
    ON scs.class_section_id = csi.class_section_id
  WHERE scsv.removed = 'FALSE'
    AND scs.removed = 'FALSE'
    AND csi.removed = 'FALSE'
  GROUP BY csi.school_id
),
child_counts AS (
  -- Aggregate child counts per school_id through child_class_section
  SELECT
    csi.school_id,
    COUNT(DISTINCT ccs.child_id) AS children_in_bubble
  FROM {{ ref('child_class_section_int') }} ccs
  INNER JOIN {{ ref('class_section_int') }} csi
    ON ccs.class_section_id = csi.class_section_id
  WHERE ccs.removed_boolean = 'FALSE'
    AND csi.removed = 'FALSE'
  GROUP BY csi.school_id
),
slot_counts AS (
  -- Aggregate slot counts per school_id
  SELECT
    school_id,
    COUNT(DISTINCT slot_id) AS slots_count
  FROM {{ ref('slot_int') }}
  WHERE removed = 'FALSE'
    AND school_id IS NOT NULL
  GROUP BY school_id
),
e2_volunteer_count AS (
  -- Count volunteers with E2 in selected_for_work_node_name
  SELECT
    COUNT(DISTINCT user_id) AS e2_volunteer_count
  FROM {{ ref('applicant_data_2025_int') }}
  WHERE selected_for_work_node_name = 'E2'
)
-- Main data for assigned partners
SELECT
  cpa.user_id,
  cpa.user_display_name as "CO Name",
  cpa.city as "City",
  cpa.user_role as "Role",
  cpa.partner_id,
  cpa.partner_name as "Partner Name",
  CASE 
    WHEN cpa.mou_start_date IS NOT NULL 
    THEN FLOOR((CURRENT_DATE - cpa.mou_start_date::date)::numeric / 7)::integer
    ELSE NULL 
  END as "Weeks Since MOU Start",
  cpa.confirmed_child_count,
  CEIL((4.0/5.0) * COALESCE(cpa.confirmed_child_count, 0)) AS volunteer_recruitment_target,
  COALESCE(svc.recruited_volunteer_count, 0) AS recruited_volunteer_count,
  COALESCE(slot_vc.assigned_volunteer_count, 0) AS assigned_volunteer_count,
  COALESCE(cc.children_in_bubble, 0) AS children_in_bubble,
  COALESCE(sc.slots_count, 0) AS slots_count
FROM co_partner_agreements cpa
LEFT JOIN crm_bubble_mapping cbm ON cpa.partner_id = cbm.crm_partner_id
LEFT JOIN school_volunteer_counts svc ON cbm.bubble_partner_id::text = svc.school_id::text
LEFT JOIN slot_volunteer_counts slot_vc ON cbm.bubble_partner_id::text = slot_vc.school_id::text
LEFT JOIN child_counts cc ON cbm.bubble_partner_id::text = cc.school_id::text
LEFT JOIN slot_counts sc ON cbm.bubble_partner_id::text = sc.school_id::text

UNION ALL

-- Unassigned E2 volunteers row
SELECT
  NULL as user_id,
  NULL as "CO Name",
  NULL as "City",
  NULL as "Role",
  NULL as partner_id,
  'Unassigned' as "Partner Name",
  NULL as "Weeks Since MOU Start",
  NULL as confirmed_child_count,
  NULL as volunteer_recruitment_target,
  e2.e2_volunteer_count as recruited_volunteer_count,
  NULL as assigned_volunteer_count,
  NULL as children_in_bubble,
  NULL as slots_count
FROM e2_volunteer_count e2

ORDER BY "Partner Name", "CO Name"