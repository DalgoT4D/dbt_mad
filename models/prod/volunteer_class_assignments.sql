{{ config(materialized='table') }}

SELECT 
    -- Volunteer Details
    ud.user_id AS volunteer_id,
    ud.user_display_name,
    ud.contact,
    ud.email,

    -- Slot Details
    s.slot_id,
    s.slot_name,
    s.academic_year AS slot_academic_year,
    s.day_of_week,
    s.start_time,
    s.end_time,
    s.reccuring,

    -- Class Section Details
    cs.class_section_id,
    cs.section_name,
    cs.academic_year AS class_section_academic_year,

    -- School Class Details
    sc.school_class_id,
    c.class_id,
    c.class_name,

    -- School/Partner Details
    p.id AS partner_id,
    p.partner_name,

    -- Subject Details
    sub.subject_id,
    sub.subject_name,

    -- Child Details
    ch.child_id,
    ch.first_name,
    ch.last_name,
    ch.gender,
    ch.dob,
    ch.city,
    ch.mad_joining_date,
    ch.date_of_enrollment,
    ch.mother_tounge,
    ch.age

FROM {{ ref('slot_class_section_volunteer') }} scsv
JOIN {{ ref('user_data') }} ud 
    ON scsv.volunteer_id = ud.user_id
JOIN {{ ref('slot_class_section') }} scs 
    ON scsv.slot_class_section_id = scs.slot_class_section_id
    AND scs.is_active = TRUE
    AND scs.removed = FALSE
JOIN {{ ref('slot') }} s 
    ON scs.slot_id = s.slot_id
JOIN {{ ref('class_section') }} cs 
    ON scs.class_section_id = cs.class_section_id
    AND cs.removed = FALSE
    AND cs.is_active = TRUE
JOIN {{ ref('school_class') }} sc 
    ON cs.school_class_id = sc.school_class_id
    AND sc.removed = FALSE
JOIN {{ ref('class') }} c 
    ON sc.class_id = c.class_id
JOIN {{ ref('partners') }} p 
    ON cs.school_id = p.id
JOIN {{ ref('class_section_subject') }} css 
    ON scs.class_section_subject_id = css.class_section_subject_id
    AND css.removed = FALSE
JOIN {{ ref('subject') }} sub 
    ON css.subject_id = sub.subject_id
    AND sub.removed = FALSE
JOIN {{ ref('child_class_section') }} ccs 
    ON cs.class_section_id = ccs.class_section_id
    AND ccs.removed = FALSE
JOIN {{ ref('child') }} ch 
    ON ccs.child_id = ch.child_id

WHERE 
    scsv.removed = FALSE 