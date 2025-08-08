{{ config(materialized='table') }}

with raw_slot as (
    select * from {{ source('bubble_staging', 'slot') }}
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from {{ source('bubble_staging', 'partner') }}
)
select
    raw."slot_id_number" as slot_id,
    raw."slot_name_text" as slot_name,
    raw."academic_year_text" as academic_year,
    raw."day_of_week_text" as day_of_week,
    raw."start_time_date" as start_time,
    raw."end_time_date" as end_time,
    raw."reccuring_boolean" as reccuring,
    partner_map.school_id,
    raw."removed_boolean" as removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot raw
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid