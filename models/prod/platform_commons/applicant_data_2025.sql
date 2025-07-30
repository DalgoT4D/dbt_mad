{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('applicant_data_2025_int') }}
