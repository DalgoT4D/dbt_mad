{{ config(materialized='table') }}

with raw_child as (
    select * from {{ source('bubble_staging', 'child') }}
),
class_map as (
    select _id as uuid, class_id_number as class_id
    from {{ source('bubble_staging', 'class') }}
),
school_class_map as (
    select _id as uuid, school_class_id_number as school_class_id
    from {{ source('bubble_staging', 'school_class') }}
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from {{ source('bubble_staging', 'partner') }}
)
select
    raw."child_id_number" as child_id,
    raw."first_name_text" as first_name,
    raw."last_name_text" as last_name,
    raw."gender_text" as gender,
    raw."dob_date" as dob,
    raw."city_text" as city,
    raw."date_of_enrollment_date" as date_of_enrollment,
    raw."mother_tounge_text" as mother_tounge,
    raw."age_number" as age,
    raw."removed_boolean" as removed,
    class_map.class_id,
    school_class_map.school_class_id,
    partner_map.school_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child raw
left join class_map on raw."class_id_custom_class" = class_map.uuid
left join school_class_map on raw."school_class_id_custom_school_class" = school_class_map.uuid
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid