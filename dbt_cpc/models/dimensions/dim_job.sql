{{ config(materialized='view') }}

select
    cast(job_id as varchar) as job_id,
    cast(job_title as varchar) as job_title,
    nullif(trim(cast(company_id as varchar)), '') as company_id,
    nullif(trim(cast(group_id as varchar)), '') as group_id,
    nullif(trim(cast(campaign_id as varchar)), '') as campaign_id,
    nullif(trim(cast(work_schedule as varchar)), '') as work_schedule,
    nullif(trim(cast(role_location as varchar)), '') as role_location,
    try_cast(nullif(trim(cast(job_budget as varchar)), '') as decimal(18, 4)) as job_budget,
    try_cast(nullif(trim(cast(status as varchar)), '') as integer) as status,
    try_cast(nullif(trim(cast(is_active as varchar)), '') as integer) as is_active
from {{ ref('seed_job') }}
