{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('fellow_applicant_data_int') }}
