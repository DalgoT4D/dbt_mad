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
FROM prod.slot_class_section_volunteer scsv
JOIN prod.slot_class_section scs 
  ON scsv.slot_class_section_id = scs.slot_class_section_id
JOIN prod.class_section cs 
  ON scs.class_section_id = cs.class_section_id
JOIN prod.user_data ud 
  ON scsv.volunteer_id = ud.user_id
JOIN prod.slot s 
  ON scs.slot_id = s.slot_id
JOIN prod.school_class sc 
  ON cs.school_class_id = sc.school_class_id
JOIN prod.class c 
  ON sc.class_id = c.class_id
JOIN prod.class_section_subject css 
  ON scs.class_section_subject_id = css.class_section_subject_id
  AND css.removed = FALSE
JOIN prod.subject sub 
  ON css.subject_id = sub.subject_id
  AND sub.removed = FALSE
JOIN prod.child_class_section ccs 
  ON cs.class_section_id = ccs.class_section_id
  AND ccs.removed = FALSE
JOIN prod.child ch 
  ON ccs.child_id = ch.child_id
JOIN prod.partners p 
  ON cs.school_id = p.id
WHERE 
    scsv.removed = FALSE
    AND scs.removed = FALSE
    AND scs.is_active = TRUE
    AND cs.removed = FALSE
    AND cs.is_active = TRUE