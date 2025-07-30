{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('students_data_int') }}
