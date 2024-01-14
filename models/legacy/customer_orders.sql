with 
--import CTEs
orders as (
    select * from {{ref('stg_orders')}}
), 
customers as (
    select * from {{ref('stg_customers')}}
), 
payments as (
    select * from {{ref('stg_payments')}}
),

--logical CTEs 

customer_orders as (
    select 
    C.customer_id
    , min(orders.order_placed_at) as first_order_date
    , max(orders.order_placed_at) as most_recent_order_date
    , count(orders.order_id) AS number_of_orders
    from customers C 
    left join orders
        on orders.customer_id = C.customer_id 
    group by 1
), 

-- final CTEs
final_cte as (
    select
    p.*,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY p.order_id
    ) as customer_sales_seq,
    CASE 
        WHEN c.first_order_date = p.order_placed_at
        THEN 'new'
        ELSE 'return' 
    END as nvsr,
    c.first_order_date as fdos
    FROM {{ref('int_orders')}} p
    left join customer_orders as c 
        USING (customer_id)
    ORDER BY order_id
)

--simple select statement 
select * from final_cte
