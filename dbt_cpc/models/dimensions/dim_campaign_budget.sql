{{ config(materialized='view') }}

select
    cast(campaign_id as varchar) as campaign_id,
    cast(campaign_name as varchar) as campaign_name,
    nullif(trim(cast(company_id as varchar)), '') as company_id,
    cast(pacing_unit as varchar) as pacing_unit,
    cast(budget_total as decimal(18, 4)) as budget_total,
    cast(daily_budget as decimal(18, 4)) as daily_budget,
    try_cast(nullif(trim(cast(is_active as varchar)), '') as integer) as is_active,
    cast(is_outlier_budget as boolean) as is_outlier_budget,
    cast(budget_source as varchar) as budget_source
from {{ ref('seed_campaign_budget') }}
