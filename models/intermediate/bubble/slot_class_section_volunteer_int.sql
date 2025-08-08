{{ config(materialized='table') }}

with raw_slot_class_section_volunteer as (
    select * from {{ source('bubble_staging', 'slot_class_section_volunteer') }}
),
slot_class_section_map as (
    select _id as uuid, volunteer_class_section_id_number as slot_class_section_id
    from {{ source('bubble_staging', 'slot_class_section') }}
),
user_map as (
    select _id as uuid, user_id_number as volunteer_id
    from {{ source('bubble_staging', 'user') }}
)
select
    raw."slot_class_section_volunteer_id_number" as slot_class_section_volunteer_id,
    slot_class_section_map.slot_class_section_id,
    user_map.volunteer_id,
    raw."academic_year_text" as academic_year,
    raw."removed_boolean" as removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot_class_section_volunteer raw
left join slot_class_section_map on raw."slot_class_section_id_custom_volunteer_class_section" = slot_class_section_map.uuid
left join user_map on raw."volunteer_id_user" = user_map.uuid