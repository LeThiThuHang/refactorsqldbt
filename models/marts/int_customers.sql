with 
customer_orders as (
    select 
    C.customer_id
    , min(orders.order_placed_at) as first_order_date
    , max(orders.order_placed_at) as most_recent_order_date
    , count(orders.order_id) AS number_of_orders
    from {{ref('stg_customers')}} C 
    left join {{ref('stg_orders')}} orders
        on orders.customer_id = C.customer_id 
    group by 1
)

select * from customer_orders
