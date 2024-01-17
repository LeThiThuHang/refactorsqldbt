with 
source as (
    select * from {{source('dbt_hle','customers')}}
), 

customers_change_name as (
    select
        FIRST_NAME    as customer_first_name,
        LAST_NAME as customer_last_name,
        ID as customer_id
    from source
)

select * from customers_change_name