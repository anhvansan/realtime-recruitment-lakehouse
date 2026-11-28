select
    cast(job_id as varchar) as job_id,
    cast(job_title as varchar) as job_title,
    cast(campaign_id as varchar) as campaign_id,
    cast(campaign_name as varchar) as campaign_name,
    cast(company_id as varchar) as company_id,
    cast(company_name as varchar) as company_name,
    cast(event_date as date) as event_date,
    cast(spend as decimal(18, 4)) as spend,
    cast(clicks as bigint) as clicks,
    cast(applies as bigint) as applies,
    cast(qualified as bigint) as qualified,
    cast(unqualified as bigint) as unqualified,
    cast(cpc as decimal(18, 4)) as cpc,
    cast(cpa as decimal(18, 4)) as cpa,
    cast(cpqa as decimal(18, 4)) as cpqa,
    cast(qualified_rate as double) as qualified_rate,
    cast(click_to_apply_rate as double) as click_to_apply_rate
from read_parquet('{gold_efficiency_path}')
where (cast(? as date) is null or event_date = cast(? as date))
order by spend desc, qualified desc, job_id asc
