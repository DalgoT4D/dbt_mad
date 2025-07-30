{{ config(materialized='table') }}

SELECT
   id::text,
   partner_name
FROM {{ source('crm_data', 'partners') }} 