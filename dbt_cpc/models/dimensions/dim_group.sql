{{ config(materialized='view') }}

select
    cast(group_id as varchar) as group_id,
    cast(group_name as varchar) as group_name,
    try_cast(nullif(trim(cast(budget_group as varchar)), '') as decimal(18, 4)) as budget_group,
    nullif(trim(cast(company_id as varchar)), '') as company_id,
    nullif(trim(cast(campaign_id as varchar)), '') as campaign_id,
    try_cast(nullif(trim(cast(status as varchar)), '') as integer) as status,
    try_cast(nullif(trim(cast(is_active as varchar)), '') as integer) as is_active
from {{ ref('seed_group') }}
