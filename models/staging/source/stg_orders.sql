with 
source as (
    select * from {{source('dbt_hle','orders')}}
), 

orders_change_name as (
    select 
        Orders.ID as order_id,
        Orders.USER_ID	as customer_id,
        Orders.ORDER_DATE AS order_placed_at,
        Orders.STATUS AS order_status
    from source
)

select * from orders_change_name