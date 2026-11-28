{{ config(materialized='view') }}

select
    cast(publisher_id as varchar) as publisher_id,
    cast(publisher_name as varchar) as publisher_name,
    try_cast(nullif(trim(cast(publisher_type as varchar)), '') as integer) as publisher_type,
    try_cast(nullif(trim(cast(min_bid as varchar)), '') as decimal(18, 4)) as min_bid,
    try_cast(nullif(trim(cast(max_bid as varchar)), '') as decimal(18, 4)) as max_bid,
    try_cast(nullif(trim(cast(cpc_increment as varchar)), '') as integer) as cpc_increment,
    try_cast(nullif(trim(cast(is_active as varchar)), '') as integer) as is_active
from {{ ref('seed_publisher') }}
