{{ config(materialized='table') }}

SELECT
    id AS id,
    partner_name AS partner_name,
    address_line_1 AS address_line_1,
    address_line_2 AS address_line_2,
    pincode AS pincode,
    partner_affiliation_type AS partner_affiliation_type,
    school_type AS school_type,
    total_child_count AS total_child_count,
    lead_source AS lead_source,
    classes AS classes,
    low_income_resource AS low_income_resource,
    created_by AS created_by,
    state_id AS state_id,
    city_id AS city_id,
    CASE
        WHEN createdAt::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        THEN createdAt::timestamptz
        ELSE NULL
    END AS createdAt,
    CASE
        WHEN updatedAt::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        THEN updatedAt::timestamptz
        ELSE NULL
    END AS updatedAt,
    interested AS interested,
    removed AS removed

FROM prod.partners 