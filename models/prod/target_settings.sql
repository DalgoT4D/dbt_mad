{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('target_settings_int') }}
