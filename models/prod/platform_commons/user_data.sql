{{ config(
  materialized = 'table'
) }}

SELECT
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
  user_updated_datetime,
  reporting_manager_user_id,
  reporting_manager_role_code,
  reporting_manager_user_login,
  'password'          AS password,          -- constant default value
  NULL                AS updated_password
FROM {{ ref('user_data_int') }}
