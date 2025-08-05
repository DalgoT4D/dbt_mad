{{ config(materialized='table') }}

with raw_class_section_subject as (
    select * from bubble_staging.class_section_subject
),
class_section_map as (
    select _id as uuid, class_section_id_number as class_section_id
    from bubble_staging.class_section
),
subject_map as (
    select _id as uuid, subject_id_number as subject_id
    from bubble_staging.subject
)
select
    raw."class_section_subject_id_number" as class_section_subject_id,
    raw."academic_year_text" as academic_year,
    class_section_map.class_section_id,
    subject_map.subject_id,
    raw."removed_boolean" as removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section_subject raw
left join class_section_map on raw."class_section_id_custom_class_section" = class_section_map.uuid
left join subject_map on raw."subject_id_custom_subject" = subject_map.uuid