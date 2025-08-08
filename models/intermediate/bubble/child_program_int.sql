{{ config(materialized='table') }}

with raw_child_program as (
    select * from {{ source('bubble_staging', 'child_program') }}
),
child_map as (
    select _id as uuid, child_id_number as child_id
    from {{ source('bubble_staging', 'child') }}
),
program_map as (
    select _id as uuid, program_id_number as program_id
    from {{ source('bubble_staging', 'program') }}
)
select
    raw."child_program_id_number" as child_program_id,
    raw."academic_year_text" as academic_year,
    child_map.child_id,
    program_map.program_id,
    raw."removed_boolean",
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_program raw
left join child_map on raw."child_id_custom_child" = child_map.uuid
left join program_map on raw."program_id1_custom_program" = program_map.uuid