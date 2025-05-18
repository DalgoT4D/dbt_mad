{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('events_data_int') }}
