{{ config(materialized='table') }}

with raw_chapter as (
    select * from {{ source('bubble_staging', 'chapter') }}
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from {{ source('bubble_staging', 'partner') }}
)
select
    raw."chapter_id_number" as chapter_id,
    raw."academic_year_text" as academic_year,
    raw."chapter_name_text" as chapter_name,
    raw."city_text" as city,
    partner_map.school_id,
    raw."state_text" as state,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_chapter raw
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid