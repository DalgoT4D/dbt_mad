{{ config(materialized='table') }}

with raw_slot_class_section as (
    select * from {{ source('bubble_staging', 'slot_class_section') }}
),
slot_map as (
    select _id as uuid, slot_id_number as slot_id
    from {{ source('bubble_staging', 'slot') }}
),
class_section_map as (
    select _id as uuid, class_section_id_number as class_section_id
    from {{ source('bubble_staging', 'class_section') }}
),
class_section_subject_map as (
    select _id as uuid, class_section_subject_id_number as class_section_subject_id
    from {{ source('bubble_staging', 'class_section_subject') }}
)
select
    raw."volunteer_class_section_id_number" as slot_class_section_id,
    slot_map.slot_id,
    class_section_map.class_section_id,
    class_section_subject_map.class_section_subject_id,
    raw."academic_year_text" as academic_year,
    raw."removed_boolean" as removed,
    raw."is_active_boolean" as is_active,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot_class_section raw
left join slot_map on raw."slot_id_custom_slot" = slot_map.uuid
left join class_section_map on raw."class_section_id_custom_class_section" = class_section_map.uuid
left join class_section_subject_map on raw."class_section_subject_id_custom_class_section_subject" = class_section_subject_map.uuid