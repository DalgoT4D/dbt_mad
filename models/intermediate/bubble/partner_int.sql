{{ config(materialized='table') }}

with raw_partner as (
    select * from bubble_staging.partner
)
select
    raw."_id" as partner_id,
    raw."city_text" as city,
    raw."Created_By" as created_by,
    raw."co_id_user" as co_id_user,
    raw."state_text" as state,
    raw."Created_Date" as created_date,
    raw."co_name_text" as co_name,
    raw."mou_url_text" as mou_url,
    raw."Modified_Date" as modified_date,
    raw."poc_name_text" as poc_name,
    raw."city_id_number" as city_id,
    raw."pincode_number" as pincode,
    raw."poc_email_text" as poc_email,
    raw."state_id_number" as state_id,
    raw."lead_source_text" as lead_source,
    raw."school_type_text" as school_type,
    raw."classes_list_text" as classes_list,
    raw."mou_end_date_date" as mou_end_date,
    raw."partner_name_text" as partner_name,
    raw."mou_sign_date_date" as mou_sign_date,
    raw."partner_id1_number" as partner_id1,
    raw."poc_contact_number" as poc_contact,
    raw."address_line_1_text" as address_line_1,
    raw."address_line_2_text" as address_line_2,
    raw."mou_start_date_date" as mou_start_date,
    raw."poc_designation_text" as poc_designation,
    raw."total_child_count_number" as total_child_count,
    raw."date_of_first_contact_date" as date_of_first_contact,
    raw."low_income_resource_boolean" as low_income_resource,
    raw."confirmed_child_count_number" as confirmed_child_count,
    raw."partner_affiliation_type_text" as partner_affiliation_type,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_partner raw 