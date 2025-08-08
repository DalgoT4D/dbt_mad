{{ config(materialized='table') }}

with raw_child_class as (
    select * from {{ source('bubble_staging', 'child_class') }}
),
child_map as (
    select _id as uuid, child_id_number as child_id
    from {{ source('bubble_staging', 'child') }}
),
school_class_map as (
    select _id as uuid, school_class_id_number as school_class_id
    from {{ source('bubble_staging', 'school_class') }}
)
select
    raw."child_class_id_number" as child_class_id,
    raw."academic_year_text" as academic_year,
    child_map.child_id,
    school_class_map.school_class_id,
    raw."removed_boolean",
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_class raw
left join child_map on raw."child_id_custom_child" = child_map.uuid
left join school_class_map on raw."school_class_id_custom_school_class" = school_class_map.uuid