{{ config(materialized='table') }}

with raw_subject as (
    select * from {{ source('bubble_staging', 'subject') }}
),
program_map as (
    select _id as uuid, program_id_number as program_id
    from {{ source('bubble_staging', 'program') }}
)
select
    raw."subject_id_number" as subject_id,
    raw."subject_name_text" as subject_name,
    raw."removed_boolean" as removed,
    program_map.program_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_subject raw
left join program_map on raw."program_id_custom_program" = program_map.uuid