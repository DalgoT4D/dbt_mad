{{ config(materialized='table') }}

with raw_user as (
    select * from {{ source('bubble_staging', 'user') }}
)
select
    raw."_id" as user_id,
    raw."city_text" as city,
    raw."state_text" as state,
    raw."center_text" as center,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."authentication",
    raw."contact_number",
    raw."user_id_number",
    raw."user_role_text" as user_role,
    raw."user_signed_up",
    raw."user_login_text" as user_login,
    raw."updated_password_text" as updated_password,
    raw."user_display_name_text" as user_display_name,
    raw."reporting_manager_role_code_text" as reporting_manager_role_code,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_user raw 