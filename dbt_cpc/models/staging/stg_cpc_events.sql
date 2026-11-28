{{ config(materialized='view') }}

with source_events as (
    select
        cast(event_id as varchar) as event_id,
        cast(event_time as timestamp) as event_time,
        cast(produced_at as timestamp) as produced_at,
        cast(processed_at as timestamp) as processed_at,
        cast(event_date as date) as event_date,
        cast(event_type as varchar) as event_type,
        cast(job_id as varchar) as job_id,
        cast(campaign_id as varchar) as campaign_id,
        cast(group_id as varchar) as group_id,
        cast(publisher_id as varchar) as publisher_id,
        cast(bid as decimal(18, 4)) as bid,
        cast(spend as decimal(18, 4)) as spend,
        cast(source_row_id as bigint) as source_row_id
    from {{ source('lakehouse_silver', 'cpc_events') }}
)

select
    event_id,
    event_time,
    date_trunc('hour', event_time) as event_hour,
    produced_at,
    processed_at,
    event_date,
    event_type,
    job_id,
    campaign_id,
    group_id,
    publisher_id,
    bid,
    spend,
    source_row_id
from source_events
