{{ config(
  materialized='table'
) }}


select
  city,
  email,
  state,
  center,
  user_id,
  contact,
  user_role,
  user_login,
  user_display_name,
  user_created_datetime,
  user_updated_datetime
from {{ref('user_data_int')}}

