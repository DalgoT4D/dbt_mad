{{ config(materialized='table') }}

with raw_co_chapter as (
    select * from bubble_staging.co_chapter
),
chapter_map as (
    select _id as uuid, chapter_id_number as chapter_id
    from bubble_staging.chapter
),
user_map as (
    select _id as uuid, user_id_number as co_id
    from bubble_staging.user
)
select
    raw."co_chapter_id_number" as co_chapter_id,
    chapter_map.chapter_id,
    raw."academic_year_text" as academic_year,
    user_map.co_id,
    raw."start_date_date" as start_date,
    raw."end_date_date" as end_date,
    raw."is_active_boolean" as is_active,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_co_chapter raw
left join chapter_map on raw."chapter_id_custom_chapter" = chapter_map.uuid
left join user_map on raw."co_id_user" = user_map.uuid