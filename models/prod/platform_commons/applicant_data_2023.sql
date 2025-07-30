{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('applicant_data_2023_int') }}
