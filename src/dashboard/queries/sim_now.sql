select
    cast(max(event_time) as timestamp) as sim_now,
    cast(max(event_time) as date) as sim_date,
    cast(max(processed_at) as timestamp) as max_processed_at,
    count(*) as total_silver_rows
from delta_scan('{silver_path}')
