{{ config(materialized='table') }}

with raw_program as (
    select * from {{ source('bubble_staging', 'program') }}
)
select
    raw."program_id_number" as program_id,
    raw."program_name_text" as program_name,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_program raw