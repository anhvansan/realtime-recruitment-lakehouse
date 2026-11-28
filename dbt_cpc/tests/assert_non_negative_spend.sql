select
    'stg_cpc_events' as source_relation,
    event_id as record_key,
    spend as violating_spend
from {{ ref('stg_cpc_events') }}
where spend < 0

union all

select
    'fct_campaign_hourly_pacing' as source_relation,
    campaign_id || '@' || cast(hour_ts as varchar) as record_key,
    least(spend_hour, cum_spend) as violating_spend
from {{ ref('fct_campaign_hourly_pacing') }}
where spend_hour < 0
   or cum_spend < 0

union all

select
    'fct_publisher_quality' as source_relation,
    publisher_id || '@' || cast(hour_ts as varchar) as record_key,
    spend as violating_spend
from {{ ref('fct_publisher_quality') }}
where spend < 0

union all

select
    'fct_recruitment_efficiency' as source_relation,
    job_id || '@' || campaign_id || '@' || cast(event_date as varchar) as record_key,
    spend as violating_spend
from {{ ref('fct_recruitment_efficiency') }}
where spend < 0
