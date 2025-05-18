{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('child_attendance') }}
