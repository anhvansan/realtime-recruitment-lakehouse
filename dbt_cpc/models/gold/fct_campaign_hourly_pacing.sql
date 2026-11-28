{{
    config(
        materialized='external',
        format='parquet',
        location="s3://" ~ env_var('S3_BUCKET', 'lakehouse') ~ "/gold/fct_campaign_hourly_pacing.parquet"
    )
}}

with bounds as (
    select
        max(event_time) as sim_now
    from {{ ref('stg_cpc_events') }}
),

campaign_days as (
    select distinct
        campaign_id,
        event_date
    from {{ ref('stg_cpc_events') }}
),

hour_spine as (
    select
        cd.campaign_id,
        cd.event_date,
        cast(cd.event_date as timestamp) + h.hour_of_day * interval '1 hour' as hour_ts,
        b.sim_now
    from campaign_days cd
    cross join bounds b
    cross join range(0, 24) as h(hour_of_day)
    where cd.event_date < cast(b.sim_now as date)
       or (
           cd.event_date = cast(b.sim_now as date)
           and h.hour_of_day <= cast(extract(hour from b.sim_now) as bigint)
       )
),

hourly_agg as (
    select
        campaign_id,
        event_date,
        event_hour as hour_ts,
        count(*) filter (where event_type = 'click') as clicks_hour,
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
        ) as spend_hour
    from {{ ref('stg_cpc_events') }}
    group by 1, 2, 3
),

trailing_60m_sim_now as (
    select
        e.campaign_id,
        cast(coalesce(sum(e.spend), 0) as decimal(18, 4)) as burn_rate_60m_sim_now
    from {{ ref('stg_cpc_events') }} e
    cross join bounds b
    where e.event_type = 'click'
      and e.event_time > b.sim_now - interval '60 minutes'
      and e.event_time <= b.sim_now
    group by 1
),

joined as (
    select
        hs.campaign_id,
        hs.event_date,
        hs.hour_ts,
        hs.sim_now,
        coalesce(ha.clicks_hour, 0) as clicks_hour,
        cast(coalesce(ha.spend_hour, 0) as decimal(18, 4)) as spend_hour,
        cast(
            sum(coalesce(ha.spend_hour, cast(0 as decimal(18, 4)))) over (
                partition by hs.campaign_id, hs.event_date
                order by hs.hour_ts
                rows between unbounded preceding and current row
            ) as decimal(18, 4)
        ) as cum_spend,
        case
            when hs.hour_ts = date_trunc('hour', hs.sim_now)
                then extract(epoch from (hs.sim_now - date_trunc('day', hs.sim_now))) / 86400.0
            else (extract(hour from hs.hour_ts) + 1) / 24.0
        end as elapsed_day_fraction,
        case
            when hs.hour_ts = date_trunc('hour', hs.sim_now)
                then cast(coalesce(t60.burn_rate_60m_sim_now, 0) as decimal(18, 4))
            else cast(coalesce(ha.spend_hour, 0) as decimal(18, 4))
        end as burn_rate,
        case
            when hs.hour_ts = date_trunc('hour', hs.sim_now)
                then (86400.0 - extract(epoch from (hs.sim_now - date_trunc('day', hs.sim_now)))) / 3600.0
            else 24.0 - (extract(hour from hs.hour_ts) + 1)
        end as hours_remaining
    from hour_spine hs
    left join hourly_agg ha
        on hs.campaign_id = ha.campaign_id
       and hs.event_date = ha.event_date
       and hs.hour_ts = ha.hour_ts
    left join trailing_60m_sim_now t60
        on hs.campaign_id = t60.campaign_id
)

select
    j.campaign_id,
    b.campaign_name,
    b.is_outlier_budget,
    b.budget_source,
    j.event_date,
    j.hour_ts,
    j.sim_now,
    j.clicks_hour,
    j.spend_hour,
    j.cum_spend,
    b.daily_budget,
    cast(cast(b.daily_budget as double) * j.elapsed_day_fraction as decimal(18, 4)) as ideal_cum_spend,
    cast(j.cum_spend as double)
        / nullif(cast(b.daily_budget as double) * j.elapsed_day_fraction, 0.0) as pacing_ratio,
    cast(j.cum_spend as double)
        / nullif(cast(b.daily_budget as double), 0.0) as budget_utilization,
    j.burn_rate,
    cast(
        cast(j.cum_spend as double) + cast(j.burn_rate as double) * j.hours_remaining
        as decimal(18, 4)
    ) as projected_eod_spend
from joined j
inner join {{ ref('dim_campaign_budget') }} b
    on j.campaign_id = b.campaign_id
