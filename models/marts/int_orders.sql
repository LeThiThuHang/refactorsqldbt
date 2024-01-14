with 
paid_orders as (
    select 
    Orders.order_id,
    Orders.customer_id,
    Orders.order_placed_at,
    Orders.order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.customer_first_name,
    C.customer_last_name
FROM{{ref('stg_orders')}} as Orders
left join {{ref('stg_payments')}} p 
    ON orders.order_ID = p.order_id
left join {{ref('stg_customers')}} C 
    on orders.customer_id = C.customer_id 
),

-- calculate cummulative value for each order

paid_orders_clv as (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 
        on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
),

final_orders as  (
    select
    p.*,
    ROW_NUMBER() OVER (
        ORDER BY p.order_id
    ) as transaction_seq,
    x.clv_bad as customer_lifetime_value
    FROM paid_orders p
    LEFT OUTER JOIN paid_orders_clv x 
        on x.order_id = p.order_id
)

select * from final_orders