{{ config(materialized='table') }}

SELECT
    donor_name,
    tip_amount,
    donor_email,
    donor_mobile,
    payment_date,
    campaign_name,
    donation_type,
    fundraiser_id,
    donation_amount,
    donation_length,
    fundraiser_name,
    payment_status,
    total_amount_paid,
    payment_campaign,
    user_updated_date_time,
    donor_campaign_code,
    fund_raise_program_name,
    payment_campaign_code,
    gateway_subscription_id,
    opportunity_id,
    donation_id,
    campaign_id

FROM {{ ref('fundraising_donations_int') }}