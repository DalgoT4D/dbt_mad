{{ config(materialized='table') }}

with child_removal_log as (
    select * from {{ source('bubble_staging', 'child_removal_log') }}
),
child as (
    select * from {{ source('bubble_staging', 'child') }}
),
user_data as (
    select * from {{ source('bubble_staging', 'user') }}
),
partner as (
    select * from {{ source('bubble_staging', 'partner') }}
)

select
    c.child_id_number as child_id,
    u.user_id_number as co_id,
    crl.child_removal_log_id_number as child_removal_log_id,
    crl.other_details_text as other_details,
    crl.removal_reason_option_student_delete_reason as removal_reason,
    crl.removed_boolean as removed,
    p.partner_id1_number as school_id,
    crl."Created_Date" as created_date,
    crl."Modified_Date" as modified_date

from child_removal_log crl

left join child c
    on c._id = crl.child_id_custom_child

left join user_data u
    on u._id = crl.co_id_user

left join partner p
    on p._id = crl.school_id_custom_partner
