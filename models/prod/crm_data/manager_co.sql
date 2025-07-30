{{ config(materialized='table') }}

SELECT
   * 
FROM {{ source('crm_data', 'manager_co') }} 