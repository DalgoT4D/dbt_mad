{{ config(
  materialized='table'
) }}

SELECT
    "CurrentStep" AS current_step,
    CAST("JoiningDate" AS DATE) AS joining_date,
    "VolunteerId" AS volunteer_id,
    "RoleAssigned" AS role_assigned,
    "OpportunityId" AS opportunity_id,
    "SourcedByUser" AS sourced_by_user,
    "SourcedMedium" AS sourced_medium,
    "SourcedSource" AS sourced_source,
    "VolunteerName" AS volunteer_name,
    CAST("UserSignUpDate" AS DATE) AS user_sign_up_date,
    "VolunteerEmail" AS volunteer_email,
    "OpportunityName" AS opportunity_name,
    "OpportunityType" AS opportunity_type,
    "SourcedByUserId" AS sourced_by_user_id,
    "SourcedCampaign" AS sourced_campaign,
    "VolunteerGender" AS volunteer_gender,
    "AttendanceStatus" AS attendance_status,
    "IsPolicyAccepted" AS is_policy_accepted,
    "VerticalAssigned" AS vertical_assigned,
    "ApplicationStatus" AS application_status,
    "CurrentStepStatus" AS current_step_status,
    "SourcedByUserLogin" AS sourced_by_user_login,
    CAST("ApplicationDateTime" AS DATE) AS application_date_time,
    "SelectedForWorkNode" AS selected_for_work_node,
    CAST("UserUpdatedDateTime" AS DATE) AS user_updated_date_time,
    "AppliedToWorkNodeName" AS applied_to_work_node_name,
    "AppliedToWorkNodeType" AS applied_to_work_node_type,
    "OpportunityApplicantId" AS opportunity_applicant_id,
    "PrimaryPreferredStream" AS primary_preferred_stream,
    "SecondaryPreferredRole" AS secondary_preferred_role,
    "SelectedForWorkNodeType" AS selected_for_work_node_type,
    "SelectedForParentWorkNode" AS selected_for_parent_work_node,
    "TeleophonicInterviewStatus" AS telephonic_interview_status,
    "SelectedForParentWorkNodeType" AS selected_for_parent_work_node_type
FROM {{ source('source_platform_commons', 'applicant_data_2023_int') }}
