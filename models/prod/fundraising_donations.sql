{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('fundraising_donations_int') }}
