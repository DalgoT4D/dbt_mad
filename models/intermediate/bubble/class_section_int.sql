{{ config(materialized='table') }}

with raw_class_section as (
    select * from bubble_staging.class_section
),
school_class_map as (
    select _id as uuid, school_class_id_number as school_class_id
    from bubble_staging.school_class
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from bubble_staging.partner
)
select
    raw."class_section_id_number" as class_section_id,
    raw."academic_year_text" as academic_year,
    raw."section_name_text" as section_name,
    raw."removed_boolean" as removed,
    raw."is_active_boolean" as is_active,
    school_class_map.school_class_id,
    partner_map.school_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section raw
left join school_class_map on raw."school_class_id1_custom_school_class" = school_class_map.uuid
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid