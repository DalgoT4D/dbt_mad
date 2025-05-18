{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('applicant_data_2024_int') }}
