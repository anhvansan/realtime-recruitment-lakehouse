with bounds as (
    select
        cast(? as timestamp) as sim_now
),

window_events as (
    select
        cast(s.publisher_id as varchar) as publisher_id,
        s.event_type,
        cast(s.spend as decimal(18, 4)) as spend
    from delta_scan('{silver_path}') s
    cross join bounds b
    where s.event_date between cast(b.sim_now - interval '60 minutes' as date) and cast(b.sim_now as date)
      and cast(s.event_time as timestamp) > b.sim_now - interval '60 minutes'
      and cast(s.event_time as timestamp) <= b.sim_now
),

agg as (
    select
        publisher_id,
        count(*) filter (where event_type = 'click') as clicks_60m,
        count(*) filter (where event_type = 'apply') as applies_60m,
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
        ) as spend_60m
    from window_events
    group by 1
),

with_rates as (
    select
        publisher_id,
        clicks_60m,
        applies_60m,
        spend_60m,
        cast(applies_60m as double) / nullif(cast(clicks_60m as double), 0.0) as click_to_apply_rate_60m
    from agg
),

with_median as (
    select
        publisher_id,
        clicks_60m,
        applies_60m,
        spend_60m,
        click_to_apply_rate_60m,
        median(click_to_apply_rate_60m) over () as median_click_to_apply_rate_60m
    from with_rates
),

publisher_dim as (
    select
        cast(publisher_id as varchar) as publisher_id,
        cast(publisher_name as varchar) as publisher_name
    from read_csv_auto('{publisher_csv}', header = true, all_varchar = true)
)

select
    w.publisher_id,
    coalesce(p.publisher_name, w.publisher_id) as publisher_name,
    w.clicks_60m,
    w.applies_60m,
    w.spend_60m,
    w.click_to_apply_rate_60m,
    w.median_click_to_apply_rate_60m,
    (w.clicks_60m >= cast(? as bigint) and w.applies_60m <= cast(? as bigint)) as fraud_flag_r1,
    (
        w.clicks_60m >= cast(? as bigint)
        and w.click_to_apply_rate_60m is not null
        and w.median_click_to_apply_rate_60m is not null
        and w.click_to_apply_rate_60m < cast(? as double) * w.median_click_to_apply_rate_60m
    ) as fraud_flag_r2
from with_median w
left join publisher_dim p
    on w.publisher_id = p.publisher_id
order by fraud_flag_r1 desc, fraud_flag_r2 desc, w.clicks_60m desc, w.publisher_id asc
