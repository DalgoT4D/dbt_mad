{{ config(materialized='table') }}

SELECT
   * 
FROM {{ ref('class_ops_master_data_int') }}
