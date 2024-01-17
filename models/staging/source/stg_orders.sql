with 
source as (
    select * from {{source('dbt_hle','orders')}}
), 

orders_change_name as (
    select 
        ID as order_id,
        USER_ID	as customer_id,
        ORDER_DATE AS order_placed_at,
        STATUS AS order_status
    from source
)

select * from orders_change_name