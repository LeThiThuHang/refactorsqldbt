with 
source as (
    select * from {{source('dbt_hle','customers')}}
), 

customers_change_name as (
    select
        customers.FIRST_NAME    as customer_first_name,
        customers.LAST_NAME as customer_last_name,
        customers.ID as customer_id
    from source
)

select * from customers_change_name