{{ config(
  materialized='table'
) }}

with formatted_data as (
    select
        date_trunc('week', "paymentDate"::timestamp) as donation_week,
        coalesce("totalAmountPaid"::numeric, 0) as total_amount
    from {{source('source_mad_donations', 'donation')}}
    where "payment_status" = 'PAID'
)

select
    donation_week,
    extract(week from donation_week) as week_number,
    to_char(donation_week, 'Month') as month_name,
    sum(total_amount) as total_weekly_donations,
    case 
        when date_trunc('week', current_date) = donation_week then 'Current Week'
        else null
    end as is_current_week
from formatted_data
group by donation_week, extract(week from donation_week), to_char(donation_week, 'Month')
order by donation_week

