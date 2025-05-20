{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('students_ids_int') }}