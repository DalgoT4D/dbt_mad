{{ config(materialized='table') }}

with cte as (SELECT
    "City" AS city,
    "State" AS state,
    "Gender" AS gender,
    "UserId" AS user_id,
    "Country" AS country,
    "Pincode" AS pincode,
    "LastName" AS last_name,
    "FirstName" AS first_name,
    "CurrentStep" AS current_step,

    CASE
        WHEN "DateOfBirth" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("DateOfBirth", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS date_of_birth,

    "DisplayName" AS display_name,
    "MobileNumber" AS mobile_number,
    "ApplicationID" AS application_id,
    "HowDidYouHear" AS how_did_you_hear,
    "OpportunityId" AS opportunity_id,
    "ReferrerMedium" AS referrer_medium,
    "ReferrerSource" AS referrer_source,
    "AreaOfResidence" AS area_of_residence,
    "ReferrerCampaign" AS referrer_campaign,
    "ApplicationStatus" AS application_status,
    "CurrentStepStatus" AS current_step_status,
    "PrimaryEmailAddress" AS primary_email_address,
    "SourcedByUserId" AS sourced_by_user_id,
    "Referrer" AS referrer,
    "ReferrerLogin" AS referrer_login,
    "CurrentlyDoing" AS currently_doing,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    CASE
        WHEN "ApplicationDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ApplicationDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS application_date_time,

    "SelectedForWorkNodeName" AS selected_for_work_node_name,
    "SelectedForWorkNodeType" AS selected_for_work_node_type,

    CASE
        WHEN "ApplicationSubmitDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ApplicationSubmitDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS application_submit_date_time,

    "SelectedForParentWorkNode" AS selected_for_parent_work_node,
    "CodeOfConductPolicyAccepted" AS code_of_conduct_policy_accepted,
    "ChildProtectionPolicyAccepted" AS child_protection_policy_accepted,
    "SelectedForParentWorkNodeType" AS selected_for_parent_work_node_type,
    
    -- Adding missing columns
    "RoleAssigned" AS role_assigned,
    "DateOfJoining" AS date_of_joining,
    "WorknodeStatus" AS worknode_status,
    "AppliedToWorknodeName" AS applied_to_worknode_name,
    "AppliedToWorknodeType" AS applied_to_worknode_type,
    "_airbyte_raw_id" AS _airbyte_raw_id,
    "_airbyte_extracted_at" AS _airbyte_extracted_at,
    "_airbyte_meta" AS _airbyte_meta

FROM {{ source('source_platform_commons', 'applicant_data_2025_int') }})

{{ dbt_utils.deduplicate(
      relation='cte',
      partition_by='application_id',
      order_by='"user_updated_date_time" desc',
     )
  }}
