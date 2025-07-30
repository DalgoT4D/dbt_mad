{{ config(materialized='table') }}

SELECT
   * 
FROM {{ source('crm_data', 'partner_agreements') }} 