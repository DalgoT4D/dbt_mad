{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('credit_data_int') }}
