{{ config(materialized='table') }}

with raw_chapter_school as (
    select * from {{ source('bubble_staging', 'chapter_school') }}
),
chapter_map as (
    select _id as uuid, chapter_id_number as chapter_id
    from {{ source('bubble_staging', 'chapter') }}
),
user_map as (
    select _id as uuid, user_id_number as co_id
    from {{ source('bubble_staging', 'user') }}
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from {{ source('bubble_staging', 'partner') }}
)
select
    raw."chapter_school_id_number" as chapter_school_id,
    chapter_map.chapter_id,
    raw."academic_year_text" as academic_year,
    user_map.co_id,
    partner_map.school_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_chapter_school raw
left join chapter_map on raw."chapter_id_custom_chapter" = chapter_map.uuid
left join user_map on raw."co_id_user" = user_map.uuid
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid