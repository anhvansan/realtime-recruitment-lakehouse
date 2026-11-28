with stg_counts as (
    select
        count(distinct event_id) as stg_distinct_event_ids
    from {{ ref('stg_cpc_events') }}
),

silver_counts as (
    select
        count(distinct event_id) as silver_distinct_event_ids
    from {{ source('lakehouse_silver', 'cpc_events') }}
)

select
    s.stg_distinct_event_ids,
    v.silver_distinct_event_ids
from stg_counts s
cross join silver_counts v
where s.stg_distinct_event_ids != v.silver_distinct_event_ids
