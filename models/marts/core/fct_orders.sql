with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ ref('stg_payments') }}
),

final as (
   select   
    orders.order_id,
    orders.order_date,
    orders.customer_id,
    sum(case when payments.status ='success' then payments.amount end) as amount
    
    from orders
    left join payments using (order_id)

    group by 1,2,3

)

select * from final


 