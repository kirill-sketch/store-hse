{{ config(distributed_replicated=true) }}

with confirmed_orders as (
    select
        order_id,
        customer_id,
        ordered_at,
        order_amount,
        row_number() over (
            partition by customer_id
            order by ordered_at, order_id
        ) as purchase_number
    from {{ ref('fct_orders') }}
    where is_confirmed
),

customer_purchases as (
    select
        customer_id,
        count(*) as confirmed_orders_count,
        sum(order_amount) as confirmed_revenue,
        max(order_id) filter (where purchase_number = 1)
            as first_confirmed_order_id,
        min(ordered_at) as first_confirmed_at
    from confirmed_orders
    group by customer_id
)

select
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    p.first_confirmed_order_id,
    p.first_confirmed_at,
    coalesce(p.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(p.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(p.confirmed_orders_count, 0) > 0 as is_buyer
from {{ ref('stg_shop__customers') }} as c
left join customer_purchases as p
    on c.customer_id = p.customer_id