{{ config(materialized='view') }}

select
    cast(company_id as varchar) as company_id,
    cast(company_name as varchar) as company_name,
    nullif(trim(cast(billing_id as varchar)), '') as billing_id,
    try_cast(nullif(trim(cast(customer_type as varchar)), '') as integer) as customer_type,
    try_cast(nullif(trim(cast(status as varchar)), '') as integer) as status,
    try_cast(nullif(trim(cast(is_active as varchar)), '') as integer) as is_active
from {{ ref('seed_company') }}
