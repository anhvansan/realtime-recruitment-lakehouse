{{
    config(
        materialized='external',
        format='parquet',
        location="s3://" ~ env_var('S3_BUCKET', 'lakehouse') ~ "/gold/fct_publisher_quality.parquet"
    )
}}

with hourly_publisher as (
    select
        publisher_id,
        event_hour as hour_ts,
        count(*) filter (where event_type = 'click') as clicks,
        count(*) filter (where event_type = 'apply') as applies,
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
    group by 1, 2
),

with_rates as (
    select
        publisher_id,
        hour_ts,
        clicks,
        applies,
        spend,
        cast(applies as double) / nullif(cast(clicks as double), 0.0) as click_to_apply_rate
    from hourly_publisher
),

with_peer_median as (
    select
        publisher_id,
        hour_ts,
        clicks,
        applies,
        spend,
        click_to_apply_rate,
        median(click_to_apply_rate) over (partition by hour_ts) as median_click_to_apply_rate
    from with_rates
)

select
    w.publisher_id,
    coalesce(p.publisher_name, w.publisher_id) as publisher_name,
    w.hour_ts,
    w.clicks,
    w.applies,
    w.spend,
    w.click_to_apply_rate,
    w.median_click_to_apply_rate,
    (w.clicks >= 100 and w.applies = 0) as fraud_flag_r1,
    (
        w.clicks >= 50
        and w.click_to_apply_rate is not null
        and w.median_click_to_apply_rate is not null
        and w.click_to_apply_rate < 0.2 * w.median_click_to_apply_rate
    ) as fraud_flag_r2
from with_peer_median w
left join {{ ref('dim_publisher') }} p
    on w.publisher_id = p.publisher_id
