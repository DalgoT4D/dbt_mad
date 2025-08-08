{{ config(materialized='table') }}

with raw_class as (
    select * from {{ source('bubble_staging', 'class') }}
)
select
    raw."class_id_number" as class_id,
    raw."class_name_text" as class_name,
    raw."program_id_number" as program_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class raw