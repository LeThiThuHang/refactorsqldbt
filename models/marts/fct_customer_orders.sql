with 
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
    left join {{ref('int_customers')}} as c 
        USING (customer_id)
    ORDER BY order_id
)

--simple select statement 
select * from final_cte


-- adding comments for feature/ci testing
