{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('user_data_int') }} 