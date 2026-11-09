with source_orders as (
    select *
    from {{ ref('orders') }}
)

select
    cast(order_id as integer) as order_id,
    trim(cast(customer_id as varchar)) as customer_id,
    try_cast(order_ts as timestamp) as order_ts,
    lower(trim(cast(status as varchar))) as status,
    try_cast(amount as decimal(18, 2)) as amount
from source_orders
