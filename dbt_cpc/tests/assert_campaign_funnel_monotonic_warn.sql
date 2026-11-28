{{ config(severity = 'warn') }}

-- Semantic note (Task 4.5 / R9):
-- Funnel monotonicity (clicks >= applies >= qualified) is INVALID at hourly or daily grain
-- because an application submitted or qualified today can originate from a click yesterday.
-- This check therefore runs ONLY at all-time cumulative campaign_id grain with severity='warn'.

with campaign_cumulative as (
    select
        campaign_id,
        count(*) filter (where event_type = 'click') as clicks,
        count(*) filter (where event_type = 'apply') as applies,
        count(*) filter (where event_type = 'qualified') as qualified
    from {{ ref('stg_cpc_events') }}
    group by 1
)

select
    campaign_id,
    clicks,
    applies,
    qualified
from campaign_cumulative
where clicks < applies
   or applies < qualified
