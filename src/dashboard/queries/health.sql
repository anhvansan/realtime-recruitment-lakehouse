with silver_stats as (
    select
        count(*) as silver_rows,
        cast(max(processed_at) as timestamp) as max_processed_at,
        extract(
            epoch from (
                cast(current_timestamp as timestamp) - max(cast(processed_at as timestamp))
            )
        ) as data_freshness_s,
        quantile_cont(
            extract(epoch from (cast(processed_at as timestamp) - cast(produced_at as timestamp))),
            0.50
        ) as e2e_latency_p50_s,
        quantile_cont(
            extract(epoch from (cast(processed_at as timestamp) - cast(produced_at as timestamp))),
            0.95
        ) as e2e_latency_p95_s
    from delta_scan('{silver_path}')
    where (cast(? as date) is null or event_date >= cast(? as date))
),

bronze_stats as (
    select
        count(*) as bronze_rows
    from delta_scan('{bronze_path}')
    where (cast(? as date) is null or event_date >= cast(? as date))
),

dlq_stats as (
    select
        count(*) as dlq_rows
    from delta_scan('{dlq_path}')
)

select
    s.silver_rows,
    b.bronze_rows,
    d.dlq_rows,
    s.max_processed_at,
    s.data_freshness_s,
    s.e2e_latency_p50_s,
    s.e2e_latency_p95_s,
    1.0 - (cast(s.silver_rows as double) / nullif(cast(b.bronze_rows as double), 0.0)) as dedup_rate,
    cast(d.dlq_rows as double) / nullif(cast(d.dlq_rows + b.bronze_rows as double), 0.0) as dlq_rate
from silver_stats s
cross join bronze_stats b
cross join dlq_stats d
