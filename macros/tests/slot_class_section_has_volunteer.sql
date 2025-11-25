{% test slot_class_section_has_volunteer(model) %}

  select count(1) as failing_rows
  from {{ model }} scs
  left join {{ ref('slot_class_section_volunteer_int') }} scsv
    on scs.slot_class_section_id = scsv.slot_class_section_id
  where scs.removed = false
    and scsv.slot_class_section_volunteer_id is null

{% endtest %}
