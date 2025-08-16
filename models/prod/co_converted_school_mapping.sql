{{ config(materialized='table') }}

-- CTE1: CO users (email, name) limited to CO roles
with cte1_co_users as (
  select distinct
    user_id,
    email as co_email,
    user_display_name as co_name
  from {{ ref('user_data_int') }}
  where user_role in ('CO Full Time', 'CO Part Time')
),

-- CTE2: School details from partners
cte2_schools as (
  select
    id as school_id,
    partner_name as school_name,
    partner_name as chapter
  from {{ ref('partners_int') }}
),

-- CTE3: Converted partners (latest agreement per partner)
cte3_converted as (
  select
    s.school_id,
    s.school_name,
    s.chapter,
    pa.id as agreement_id,
    pa.updated_at,
    row_number() over (partition by pa.partner_id order by pa.updated_at desc) as rn
  from cte2_schools s
  join {{ ref('partner_agreements_int') }} pa
    on s.school_id = pa.partner_id
  where pa.conversion_stage = 'converted' and coalesce(pa.removed, 'FALSE') = 'FALSE'
),

-- CTE4: Map converted schools to COs via partner assignments
cte4_school_cos as (
  select
    pc.co_id,
    c.school_id,
    c.school_name,
    c.chapter
  from {{ ref('partner_cos_int') }} pc
  left join (
    select school_id, school_name, chapter
    from cte3_converted
    where rn = 1
  ) c
    on pc.partner_id = c.school_id
)

-- Final: Join CO users with their converted schools
select distinct
  u.co_email,
  u.co_name,
  sc.chapter,
  sc.school_id,
  sc.school_name
from cte1_co_users u
join cte4_school_cos sc
  on u.user_id = sc.co_id
where sc.school_id is not null

