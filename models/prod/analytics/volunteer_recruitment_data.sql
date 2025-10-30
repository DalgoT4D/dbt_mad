{{
  config(
    materialized='table',
    schema='analytics',
    description='Volunteer recruitment data with partner details, CO information, confirmed child counts from CRM, volunteer recruitment targets (4/5 of confirmed child count), and volunteer assignment metrics'
  )
}}

-- Active partners with latest converted agreement
WITH active_partners AS (
    SELECT 
        p.id AS partner_id,
        p.partner_name,
        p.created_by AS co_user_id
    FROM {{ ref('partners_int') }} p
    WHERE p.removed = false
),

-- Latest converted agreement per partner
converted_agreements AS (
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

-- MOU details for each partner with confirmed child count (deduplicated)
mou_details AS (
    SELECT 
        partner_id,
        confirmed_child_count
    FROM (
        SELECT 
            id,
            partner_id,
            confirmed_child_count,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('mous_int') }}
    ) ranked
    WHERE rn = 1
),

-- Community Organizer (CO) details (deduplicated)
co_details AS (
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

-- Volunteer count per school
volunteer_counts AS (
    SELECT 
        sv.school_id,
        COUNT(*) AS volunteer_count
    FROM {{ ref('school_volunteer_int') }} sv
    WHERE sv.removed = false
    GROUP BY sv.school_id
),

-- Volunteers assigned to classes (through slot assignments)
volunteers_assigned_to_class AS (
    SELECT 
        cs.school_id,
        COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned_to_class
    FROM {{ ref('slot_class_section_volunteer_int') }} scsv
    INNER JOIN {{ ref('slot_class_section_int') }} scs 
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('class_section_int') }} cs 
        ON scs.class_section_id = cs.class_section_id
    WHERE scsv.removed = false 
      AND scs.removed = false 
      AND cs.removed = false
    GROUP BY cs.school_id
)

-- Main query combining all data
SELECT
    -- Partner Information
    ap.partner_id::text AS "Partner ID",
    ap.partner_name AS "Partner Name",
    
    -- Community Organizer Details
    cd.co_id AS "CO ID",
    cd.co_name AS "CO Name",
    
    -- Confirmed Child Count from CRM (MOU)
    COALESCE(md.confirmed_child_count, 0) AS "Confirmed Child Count (CRM)",
    
    -- Volunteer Recruitment Target (4/5 times confirmed child count)
    CEIL(COALESCE(md.confirmed_child_count, 0) * 4.0 / 5.0) AS "Volunteer Recruitment Target",
    
    -- Volunteers Recruited
    COALESCE(vc.volunteer_count, 0) AS "Volunteers Recruited",
    
    -- Volunteers Assigned to Class
    COALESCE(vac.volunteers_assigned_to_class, 0) AS "Volunteers Assigned to Class",
    
    -- Percentage Volunteers Assigned to School (Recruited/Target)
    CASE 
        WHEN CEIL(COALESCE(md.confirmed_child_count, 0) * 4.0 / 5.0) > 0 
        THEN ROUND(
            (COALESCE(vc.volunteer_count, 0)::numeric / CEIL(COALESCE(md.confirmed_child_count, 0) * 4.0 / 5.0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Volunteers Assigned to School",
    
    -- Percentage Volunteers Assigned to Class (Assigned to Class/Recruited)
    CASE 
        WHEN COALESCE(vc.volunteer_count, 0) > 0 
        THEN ROUND(
            (COALESCE(vac.volunteers_assigned_to_class, 0)::numeric / COALESCE(vc.volunteer_count, 0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Volunteers Assigned to Class"

FROM active_partners ap

-- Join latest converted agreement
INNER JOIN latest_agreements la
    ON ap.partner_id::text = la.partner_id::text

-- Join MOU details
LEFT JOIN mou_details md 
    ON md.partner_id = ap.partner_id

-- Join CO details
LEFT JOIN co_details cd 
    ON cd.co_id = ap.co_user_id

-- Join volunteer counts (assigned to school)
LEFT JOIN volunteer_counts vc 
    ON vc.school_id = ap.partner_id::numeric

-- Join volunteers assigned to class
LEFT JOIN volunteers_assigned_to_class vac 
    ON vac.school_id = ap.partner_id::numeric

-- Order by partner name
ORDER BY ap.partner_name
