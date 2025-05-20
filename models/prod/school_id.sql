{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('school_id_int') }}