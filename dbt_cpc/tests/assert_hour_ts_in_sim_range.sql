with sim_bounds as (
    select
        cast(min(event_date) as timestamp) as min_sim_ts,
        date_trunc('hour', max(event_time)) as max_sim_hour
    from {{ ref('stg_cpc_events') }}
)

select
    p.campaign_id,
    p.event_date,
    p.hour_ts,
    b.min_sim_ts,
    b.max_sim_hour
from {{ ref('fct_campaign_hourly_pacing') }} p
cross join sim_bounds b
where p.hour_ts < b.min_sim_ts
   or p.hour_ts > b.max_sim_hour
