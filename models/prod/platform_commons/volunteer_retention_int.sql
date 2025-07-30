{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('volunteer_retention_d_int') }}
