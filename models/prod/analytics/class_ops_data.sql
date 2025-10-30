{{
  config(
    materialized='table',
    schema='analytics',
    description='Class operations data with partner details, CO information, children in bubble count, and volunteer assignment metrics'
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

-- Children in Bubble from Bubble active children (child_int)
mou_details AS (
    SELECT 
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false 
      AND is_active = true
    GROUP BY school_id
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
),

-- Current slot count per school
current_slot_counts AS (
    SELECT
        school_id,
        COUNT(DISTINCT slot_id) AS current_slot_count
    FROM {{ ref('slot_int') }}
    WHERE removed = false
      AND school_id IS NOT NULL
    GROUP BY school_id
),

-- Current class count per school
current_class_counts AS (
    SELECT
        school_id,
        COUNT(DISTINCT class_section_id) AS current_class_count
    FROM {{ ref('class_section_int') }}
    WHERE removed = false
      AND is_active = true
      AND school_id IS NOT NULL
    GROUP BY school_id
),

-- Classes with 1 volunteer assigned
classes_with_1_volunteer AS (
    SELECT 
        cs.school_id,
        COUNT(*) AS classes_with_1_volunteer_count
    FROM (
        SELECT 
            cs.school_id,
            cs.class_section_id
        FROM {{ ref('class_section_int') }} cs
        INNER JOIN {{ ref('slot_class_section_int') }} scs 
            ON cs.class_section_id = scs.class_section_id
        INNER JOIN {{ ref('slot_class_section_volunteer_int') }} scsv 
            ON scs.slot_class_section_id = scsv.slot_class_section_id
        WHERE cs.removed = false 
          AND cs.is_active = true
          AND scs.removed = false
          AND scsv.removed = false
        GROUP BY cs.school_id, cs.class_section_id
        HAVING COUNT(DISTINCT scsv.volunteer_id) = 1
    ) cs
    GROUP BY cs.school_id
),

-- Classes with 2 volunteers assigned
classes_with_2_volunteers AS (
    SELECT 
        cs.school_id,
        COUNT(*) AS classes_with_2_volunteers_count
    FROM (
        SELECT 
            cs.school_id,
            cs.class_section_id
        FROM {{ ref('class_section_int') }} cs
        INNER JOIN {{ ref('slot_class_section_int') }} scs 
            ON cs.class_section_id = scs.class_section_id
        INNER JOIN {{ ref('slot_class_section_volunteer_int') }} scsv 
            ON scs.slot_class_section_id = scsv.slot_class_section_id
        WHERE cs.removed = false 
          AND cs.is_active = true
          AND scs.removed = false
          AND scsv.removed = false
        GROUP BY cs.school_id, cs.class_section_id
        HAVING COUNT(DISTINCT scsv.volunteer_id) = 2
    ) cs
    GROUP BY cs.school_id
),

-- Average slot duration per school
-- First calculate average duration for each individual slot, then average across all slots
average_slot_duration AS (
    SELECT
        school_id,
        AVG(individual_slot_avg_duration) AS avg_slot_duration_minutes
    FROM (
        SELECT
            school_id,
            slot_id,
            AVG(
                EXTRACT(EPOCH FROM (end_time::timestamp - start_time::timestamp)) / 60
            ) AS individual_slot_avg_duration
        FROM {{ ref('slot_int') }}
        WHERE removed = false
          AND school_id IS NOT NULL
          AND start_time IS NOT NULL
          AND end_time IS NOT NULL
        GROUP BY school_id, slot_id
    ) slot_averages
    GROUP BY school_id
)

-- Main query combining all data
SELECT
    -- Partner Information
    ap.partner_id::text AS "Partner ID",
    ap.partner_name AS "Partner Name",
    
    -- Community Organizer Details
    cd.co_id AS "CO ID",
    cd.co_name AS "CO Name",
    
    -- Children in Bubble (Active children from Bubble)
    COALESCE(md.active_child_count, 0) AS "Children in Bubble",
    
    -- Volunteers Recruited
    COALESCE(vc.volunteer_count, 0) AS "Volunteers Recruited",
    
    -- Volunteers Assigned to Class
    COALESCE(vac.volunteers_assigned_to_class, 0) AS "Volunteers Assigned to Class",
    
    -- Ideal slot count (set to 2 for all partners)
    2 AS "Ideal Slot Count",
    
    -- Current slot count
    COALESCE(csc.current_slot_count, 0) AS "Current Slot Count",
    
    -- Ideal class count (2/5 times children in bubble)
    CEIL(COALESCE(md.active_child_count, 0) * 2.0 / 5.0) AS "Ideal Class Count",
    
    -- Current class count
    COALESCE(ccc.current_class_count, 0) AS "Current Class Count",
    
    -- Classes with 1 volunteer assigned
    COALESCE(c1v.classes_with_1_volunteer_count, 0) AS "Classes with 1 Volunteer",
    
    -- Classes with 2 volunteers assigned
    COALESCE(c2v.classes_with_2_volunteers_count, 0) AS "Classes with 2 Volunteers",
    
    -- Percentage classes vs ideal classes (Current Class Count / Ideal Class Count * 100)
    CASE 
        WHEN CEIL(COALESCE(md.active_child_count, 0) * 2.0 / 5.0) > 0 
        THEN ROUND(
            (COALESCE(ccc.current_class_count, 0)::numeric / CEIL(COALESCE(md.active_child_count, 0) * 2.0 / 5.0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes vs Ideal Classes",
    
    -- Percentage classes with at least 1 volunteer ((Classes with 1 + Classes with 2) / Current Class Count * 100)
    CASE 
        WHEN COALESCE(ccc.current_class_count, 0) > 0 
        THEN ROUND(
            ((COALESCE(c1v.classes_with_1_volunteer_count, 0) + COALESCE(c2v.classes_with_2_volunteers_count, 0))::numeric / COALESCE(ccc.current_class_count, 0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes with At Least 1 Volunteer",
    
    -- Percentage classes with 2 volunteers (Classes with 2 / Current Class Count * 100)
    CASE 
        WHEN COALESCE(ccc.current_class_count, 0) > 0 
        THEN ROUND(
            (COALESCE(c2v.classes_with_2_volunteers_count, 0)::numeric / COALESCE(ccc.current_class_count, 0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes with 2 Volunteers",
    
    -- Average slot duration in minutes
    ROUND(COALESCE(asd.avg_slot_duration_minutes, 0), 2) AS "Average Slot Duration (Minutes)"

FROM active_partners ap

-- Join latest converted agreement
INNER JOIN latest_agreements la
    ON ap.partner_id::text = la.partner_id::text

-- Join MOU details
LEFT JOIN mou_details md 
    ON md.school_id = ap.partner_id::numeric

-- Join CO details
LEFT JOIN co_details cd 
    ON cd.co_id = ap.co_user_id

-- Join volunteer counts (assigned to school)
LEFT JOIN volunteer_counts vc 
    ON vc.school_id = ap.partner_id::numeric

-- Join volunteers assigned to class
LEFT JOIN volunteers_assigned_to_class vac 
    ON vac.school_id = ap.partner_id::numeric

-- Join current slot counts
LEFT JOIN current_slot_counts csc 
    ON csc.school_id = ap.partner_id::numeric

-- Join current class counts
LEFT JOIN current_class_counts ccc 
    ON ccc.school_id = ap.partner_id::numeric

-- Join classes with 1 volunteer
LEFT JOIN classes_with_1_volunteer c1v 
    ON c1v.school_id = ap.partner_id::numeric

-- Join classes with 2 volunteers
LEFT JOIN classes_with_2_volunteers c2v 
    ON c2v.school_id = ap.partner_id::numeric

-- Join average slot duration
LEFT JOIN average_slot_duration asd 
    ON asd.school_id = ap.partner_id::numeric

-- Order by partner name
ORDER BY ap.partner_name
