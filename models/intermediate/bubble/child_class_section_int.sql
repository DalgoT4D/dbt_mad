{{ config(materialized='table') }}

with raw_child_class_section as (
    select * from bubble_staging.child_class_section
),
child_map as (
    select _id as uuid, child_id_number as child_id
    from bubble_staging.child
),
class_section_map as (
    select _id as uuid, class_section_id_number as class_section_id
    from bubble_staging.class_section
)
select
    raw."child_class_section_id_number" as child_class_section_id,
    raw."academic_year_text" as academic_year,
    child_map.child_id,
    class_section_map.class_section_id,
    raw."removed_boolean",
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_class_section raw
left join child_map on raw."child_id_custom_child" = child_map.uuid
left join class_section_map on raw."class_section_id_custom_class_section" = class_section_map.uuid