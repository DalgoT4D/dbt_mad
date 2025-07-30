{{ config(materialized='view') }}

SELECT 
    scsv.volunteer_id,
    scs.slot_id,
    scs.class_section_subject_id,
    cs.section_name,
    p.id AS partner_id,
    p.partner_name,
    ud.user_id,
    ud.user_display_name,
    ud.contact,
    ud.email,
    s.day_of_week,
    ch.child_id,
    ch.first_name AS child_first_name,
    ch.last_name AS child_last_name,
    c.class_name,
    sub.subject_name
FROM {{ ref('slot_class_section_volunteer_int') }} scsv
JOIN {{ ref('slot_class_section_int') }} scs 
  ON scsv.slot_class_section_id::numeric = scs.volunteer_class_section_id
JOIN {{ ref('class_section_int') }} cs 
  ON scs.class_section_id = cs.class_section_id::text
JOIN {{ ref('user_data_int') }} ud 
  ON scsv.volunteer_id::numeric = ud.user_id
JOIN {{ ref('slot_int') }} s 
  ON scs.slot_id = s.slot_id::text
JOIN {{ ref('school_class_int') }} sc 
  ON cs.school_class_id = sc.school_class_id::text
JOIN {{ ref('class_int') }} c 
  ON sc.class_id = c.class_id::text
JOIN {{ ref('class_section_subject_int') }} css 
  ON scs.class_section_subject_id = css.class_section_subject_id::text
  AND css.is_removed = FALSE
JOIN {{ ref('subject_int') }} sub 
  ON css.subject_id = sub.subject_id::text
  AND sub.is_removed = FALSE
JOIN {{ ref('child_class_section_int') }} ccs 
  ON cs.class_section_id::text = ccs.class_section_id
  AND ccs.is_removed = FALSE
JOIN {{ ref('child_int') }} ch 
  ON ccs.child_id = ch.child_id::text
JOIN {{ ref('partners_int') }} p 
  ON cs.school_id = p.id::text
WHERE 
    scsv.is_removed = FALSE
    AND scs.is_removed = FALSE
    AND scs.is_active = TRUE
    AND cs.is_removed = FALSE
    AND cs.is_active = TRUE 