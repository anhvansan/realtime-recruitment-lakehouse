select
    campaign_id,
    event_date,
    hour_ts,
    cum_spend,
    ideal_cum_spend,
    pacing_ratio
from {{ ref('fct_campaign_hourly_pacing') }}
where pacing_ratio is not null
  and pacing_ratio < 0
