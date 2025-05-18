{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('mad_opportunity_int') }}
