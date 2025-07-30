{{ config(materialized='table') }}

SELECT
   * 
FROM {{ source('crm_data', 'poc_partners') }} 