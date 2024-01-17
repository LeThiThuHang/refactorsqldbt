with 
source as (
    select * from {{source('dbt_hle','payments')}}
), 

payments_not_fail as (
    select 
    ORDERID as order_id, 
    max(CREATED) as payment_finalized_date, 
    sum(AMOUNT) / 100.0 as total_amount_paid
    from source 
    where STATUS <> 'fail'
    group by 1
)

select * from payments_not_fail