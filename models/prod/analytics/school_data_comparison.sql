{{ config(materialized='table') }}
-- School Data Comparison: Partner schools with CO details and child count metrics
-- Shows:
-- - Community Organizer details (ID and name)
-- - Partner name and MOU information
-- - Child count metrics: confirmed (CRM), active (Bubble), and dropped (Bubble)

WITH partners AS (
    -- Base partner data
    SELECT
        id AS partner_id,
        partner_name,
        created_by AS co_user_id,
        removed
    FROM {{ ref('partners_int') }}
    WHERE removed = false
),

converted_agreements AS (
    -- Latest converted agreement per partner
    SELECT
        id,
        partner_id,
        conversion_stage,
        created_at
    FROM {{ ref('partner_agreements_int') }}
    WHERE conversion_stage = 'converted'
),

latest_agreements AS (
    SELECT
        partner_id,
        conversion_stage,
        created_at
    FROM (
        SELECT
            partner_id,
            conversion_stage,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM converted_agreements
    ) ranked
    WHERE rn = 1
),

mou_data AS (
    -- MOU details with confirmed child count (deduplicated)
    SELECT
        partner_id,
        mou_sign_date,
        confirmed_child_count,
        weeks_since_mou_signed
    FROM (
        SELECT
            id,
            partner_id,
            mou_sign_date,
            confirmed_child_count,
            CASE 
                WHEN mou_sign_date IS NOT NULL 
                THEN FLOOR((CURRENT_DATE - mou_sign_date::date)::numeric / 7)::integer
                ELSE NULL 
            END AS weeks_since_mou_signed,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('mous_int') }}
    ) ranked
    WHERE rn = 1
),

community_organizers AS (
    -- Community Organizer details (deduplicated)
    SELECT
        user_id AS co_id,
        user_display_name AS co_name
    FROM (
        SELECT
            user_id,
            user_display_name,
            ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY user_updated_datetime DESC, user_created_datetime DESC
            ) as rn
        FROM {{ ref('user_data_int') }}
    ) ranked
    WHERE rn = 1
),

active_children AS (
    -- Active children count from Bubble
    SELECT 
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false 
      AND is_active = true
    GROUP BY school_id
),

dropped_children AS (
    -- Dropped children count from Bubble
    SELECT 
        school_id,
        COUNT(*) AS dropped_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false 
      AND is_active = false
    GROUP BY school_id
)

-- Schools in CRM (with converted agreements)
SELECT
    -- Partner Information
    p.partner_id::text AS "Partner ID",
    p.partner_name AS "Partner Name",
    
    -- Community Organizer Details
    co.co_id AS "CO ID",
    co.co_name AS "CO Name",
    
    -- MOU Information
    m.mou_sign_date AS "MOU Sign Date",
    m.weeks_since_mou_signed AS "Weeks Since MOU Signed",
    
    -- Child Count Metrics
    m.confirmed_child_count AS "Confirmed Child Count (CRM)",
    COALESCE(ac.active_child_count, 0) AS "Active Child Count (Bubble)",
    COALESCE(dc.dropped_child_count, 0) AS "Dropped Child Count (Bubble)",
    
    -- Platform Presence
    CASE 
        WHEN ac.active_child_count > 0 OR dc.dropped_child_count > 0 
        THEN 'BOTH'
        ELSE 'CRM'
    END AS "Platform Presence",
    
    -- CRM Status (numeric indicator)
    100 AS "CRM Status",
    
    -- Child Count Ratio (Bubble / CRM)
    CASE 
        WHEN m.confirmed_child_count > 0 
        THEN ROUND(
            (COALESCE(ac.active_child_count, 0)::numeric / m.confirmed_child_count::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Child Count Ratio (Bubble / CRM)"

FROM partners p

-- Join with latest converted agreements
INNER JOIN latest_agreements la
    ON p.partner_id::text = la.partner_id::text

-- Join with MOU data
LEFT JOIN mou_data m
    ON p.partner_id::text = m.partner_id::text

-- Join with Community Organizer details
LEFT JOIN community_organizers co
    ON p.co_user_id::text = co.co_id::text

-- Join with active children count
LEFT JOIN active_children ac
    ON p.partner_id::numeric = ac.school_id::numeric

-- Join with dropped children count
LEFT JOIN dropped_children dc
    ON p.partner_id::numeric = dc.school_id::numeric

UNION ALL

-- Schools in Bubble but not in CRM
SELECT
    -- Partner Information (use school ID and name from Bubble)
    bp.partner_id1::text AS "Partner ID",
    bp.partner_name AS "Partner Name",
    
    -- Community Organizer Details (NULL for Bubble-only schools)
    NULL AS "CO ID",
    NULL AS "CO Name",
    
    -- MOU Information (NULL for Bubble-only schools)
    NULL AS "MOU Sign Date",
    NULL AS "Weeks Since MOU Signed",
    
    -- Child Count Metrics
    NULL AS "Confirmed Child Count (CRM)",
    COALESCE(ac.active_child_count, 0) AS "Active Child Count (Bubble)",
    COALESCE(dc.dropped_child_count, 0) AS "Dropped Child Count (Bubble)",
    
    -- Platform Presence
    'BUBBLE' AS "Platform Presence",
    
    -- CRM Status (numeric indicator)
    0 AS "CRM Status",
    
    -- Child Count Ratio (Bubble / CRM) - NULL for Bubble-only schools
    NULL AS "Child Count Ratio (Bubble / CRM)"

FROM {{ ref('partner_int') }} bp

-- Join with active children count
LEFT JOIN active_children ac
    ON bp.partner_id1::numeric = ac.school_id::numeric

-- Join with dropped children count
LEFT JOIN dropped_children dc
    ON bp.partner_id1::numeric = dc.school_id::numeric

-- Exclude schools that are already in CRM
LEFT JOIN partners p
    ON bp.partner_name = p.partner_name

WHERE p.partner_id IS NULL  -- Only include schools not in CRM
  AND bp.partner_name IS NOT NULL
  AND (ac.active_child_count > 0 OR dc.dropped_child_count > 0)  -- Only include schools with children
