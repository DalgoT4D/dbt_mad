{{ config(
  materialized='table'
) }}

-- CTE1: Get distinct user data for CO roles with targets
with user_data as (
  select distinct
    user_id,
    user_display_name,
    user_role,
    city,
    case 
      when user_role = 'CO Full Time' then 5
      when user_role = 'CO Part Time' then 1
      else 0
    end as mou_target,
    case 
      when user_role = 'CO Full Time' then 5 * 50
      when user_role = 'CO Part Time' then 1 * 50
      else 0
    end as child_count_target
  from {{ ref('user_data_int') }}
  where user_role in ('CO Full Time', 'CO Part Time')
),

-- CTE2: Get latest partner agreements for each partner
latest_partner_agreements as (
  select 
    partner_id,
    conversion_stage,
    updated_at
  from (
    select 
      partner_id,
      conversion_stage,
      updated_at,
      row_number() over (partition by partner_id order by updated_at desc) as rn
    from {{ ref('partner_agreements_int') }}
    where removed = 'FALSE'
  ) ranked
  where rn = 1
),

-- CTE3: Join MOU data with latest partner agreements
mou_with_agreements as (
  select 
    m.partner_id,
    m.confirmed_child_count,
    pa.conversion_stage
  from {{ ref('mous_int') }} m
  left join latest_partner_agreements pa 
    on m.partner_id = pa.partner_id
),

-- CTE4: Aggregate at CO level with partner assignments
co_aggregated as (
  select 
    pco.co_id,
    count(case when mou.conversion_stage = 'converted' then 1 end) as mou_signed,
    count(case when mou.conversion_stage in ('dropped', 'not interested') then 1 end) as lead_lost,
    count(case when mou.conversion_stage not in ('converted', 'dropped', 'not interested') then 1 end) as lead_active,
    coalesce(sum(mou.confirmed_child_count), 0) as converted_child_count
  from {{ ref('partner_cos_int') }} pco
  left join mou_with_agreements mou 
    on pco.partner_id = mou.partner_id
  group by pco.co_id
)

-- Final join: CO performance data with user targets
select 
  ud.user_id,
  ud.user_display_name,
  ud.user_role,
  ud.city,
  ud.mou_target,
  ud.child_count_target,
  coalesce(ca.mou_signed, 0) as mou_signed,
  coalesce(ca.lead_lost, 0) as lead_lost,
  coalesce(ca.lead_active, 0) as lead_active,
  coalesce(ca.converted_child_count, 0) as converted_child_count
from user_data ud
left join co_aggregated ca 
  on ud.user_id = ca.co_id