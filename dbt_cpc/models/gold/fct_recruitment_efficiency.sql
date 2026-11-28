{{
    config(
        materialized='external',
        format='parquet',
        location="s3://" ~ env_var('S3_BUCKET', 'lakehouse') ~ "/gold/fct_recruitment_efficiency.parquet"
    )
}}

with job_campaign_daily as (
    select
        job_id,
        campaign_id,
        event_date,
        count(*) filter (where event_type = 'click') as clicks,
        count(*) filter (where event_type = 'apply') as applies,
        count(*) filter (where event_type = 'qualified') as qualified,
        count(*) filter (where event_type = 'unqualified') as unqualified,
        cast(
            coalesce(
                sum(
                    case
                        when event_type = 'click' then spend
                        else cast(0 as decimal(18, 4))
                    end
                ),
                0
            ) as decimal(18, 4)
        ) as spend
    from {{ ref('stg_cpc_events') }}
    group by 1, 2, 3
)

select
    a.job_id,
    j.job_title,
    a.campaign_id,
    c.campaign_name,
    coalesce(j.company_id, c.company_id) as company_id,
    comp.company_name,
    a.event_date,
    a.spend,
    a.clicks,
    a.applies,
    a.qualified,
    a.unqualified,
    cast(cast(a.spend as double) / nullif(cast(a.clicks as double), 0.0) as decimal(18, 4)) as cpc,
    cast(cast(a.spend as double) / nullif(cast(a.applies as double), 0.0) as decimal(18, 4)) as cpa,
    cast(cast(a.spend as double) / nullif(cast(a.qualified as double), 0.0) as decimal(18, 4)) as cpqa,
    cast(a.qualified as double) / nullif(cast(a.applies as double), 0.0) as qualified_rate,
    cast(a.applies as double) / nullif(cast(a.clicks as double), 0.0) as click_to_apply_rate
from job_campaign_daily a
left join {{ ref('dim_job') }} j
    on a.job_id = j.job_id
left join {{ ref('dim_campaign_budget') }} c
    on a.campaign_id = c.campaign_id
left join {{ ref('dim_company') }} comp
    on coalesce(j.company_id, c.company_id) = comp.company_id
