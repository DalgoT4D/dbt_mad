{{ config(materialized='table') }}

with raw_batch_child as (
    select * from bubble_staging.batch_child
),
child_map as (
    select _id as uuid, child_id_number as child_id
    from bubble_staging.child
),
partner_map as (
    select _id as uuid, partner_id1_number as school_id
    from bubble_staging.partner
)
select
    raw."batch_child_id_number" as batch_child_id,
    raw."academic_year_text" as academic_year,
    child_map.child_id,
    partner_map.school_id,
    raw."removed_boolean",
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_batch_child raw
left join child_map on raw."child_id_custom_child" = child_map.uuid
left join partner_map on raw."school_id_custom_partner" = partner_map.uuid