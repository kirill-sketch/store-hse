with successful_payments as (
    select
        order_id
    from {{ source('shop', 'payments') }}
    where payment_status = 'success'
      and amount > 0
    group by order_id
),

source_totals as (
    select
        count(*) as orders_count,
        coalesce(sum(o.total_amount), 0) as revenue
    from {{ source('shop', 'orders') }} as o
    join successful_payments as p
        on o.order_id = p.order_id
    where o.status = 'paid'
),

mart_totals as (
    select
        count(*) as orders_count,
        coalesce(sum(order_amount), 0) as revenue
    from {{ ref('fct_orders') }}
    where is_confirmed
)

select
    s.orders_count as source_orders,
    m.orders_count as mart_orders,
    s.revenue as source_revenue,
    m.revenue as mart_revenue
from source_totals as s
cross join mart_totals as m
where s.orders_count <> m.orders_count
   or s.revenue <> m.revenue