{{ config(materialized='table') }}

with raw_school_volunteer as (
    select * from {{ source('bubble_staging', 'school_volunteer') }}
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from {{ source('bubble_staging', 'partner') }}
),
user_map as (
    select _id as uuid, user_id_number as volunteer_id
    from {{ source('bubble_staging', 'user') }}
)
select
    raw."school_volunteer_id_number" as school_volunteer_id,
    raw."academic_year_text" as academic_year,
    partner_map.school_id,
    user_map.volunteer_id,
    raw."removed_boolean" as removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_volunteer raw
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid
left join user_map on raw."volunteer_id_user" = user_map.uuid