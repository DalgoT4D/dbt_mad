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

-- CTE2: Filter active partners (not removed)
active_partners as (
  select 
    id as partner_id
  from {{ ref('partners_int') }}
  where removed = 'FALSE'
),

-- CTE3: Get latest partner agreements for each active partner
latest_partner_agreements as (
  select 
    ranked.partner_id,
    ranked.conversion_stage,
    ranked.updated_at
  from (
    select 
      partner_id,
      conversion_stage,
      updated_at,
      row_number() over (partition by partner_id order by updated_at desc) as rn
    from {{ ref('partner_agreements_int') }}
    where removed = 'FALSE'
  ) ranked
  inner join active_partners ap
    on  ap.partner_id = ranked.partner_id
  where ranked.rn = 1
),

-- CTE4: Join latest partner agreements with MOU data
mou_with_agreements as (
  select 
    pa.partner_id,
    m.confirmed_child_count,
    pa.conversion_stage
  from latest_partner_agreements pa
  left join {{ ref('mous_int') }} m
    on pa.partner_id = m.partner_id
),

-- CTE5: Aggregate at CO level with partner assignments
co_aggregated as (
  select 
    pco.co_id,
    count(case when mou.conversion_stage = 'converted' then 1 end) as mou_signed,
    count(case when mou.conversion_stage in ('dropped', 'not interested') then 1 end) as lead_lost,
    count(case when mou.conversion_stage not in ('converted', 'dropped', 'not interested') then 1 end) as lead_active,
    coalesce(sum(mou.confirmed_child_count), 0) as converted_child_count
  from mou_with_agreements mou
  left join {{ ref('partner_cos_int') }} pco
    on mou.partner_id = pco.partner_id
  inner join active_partners ap
    on mou.partner_id = ap.partner_id
  group by pco.co_id
)

-- Final join: CO performance data with user targets
select 
  ud.user_id,
  ud.user_display_name as "CO Name",
  ud.user_role,
  ud.city as "City",
  ud.mou_target,
  ud.child_count_target,
  coalesce(ca.mou_signed, 0) as mou_signed,
  coalesce(ca.lead_lost, 0) as lead_lost,
  coalesce(ca.lead_active, 0) as lead_active,
  coalesce(ca.converted_child_count, 0) as converted_child_count
from user_data ud
left join co_aggregated ca 
  on ud.user_id = ca.co_id