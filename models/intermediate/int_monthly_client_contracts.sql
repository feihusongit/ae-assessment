with recursive months(m) as (

        select date('2024-01-01', 'start of month')
        union all
        select date(m, '+1 month')
        from months
        where m < date(current_date, 'start of month')

    ),

    contracts as (

        select * from {{ ref('stg_client_contracts') }} 
    )

    clients as (

        select distinct client_id from contracts

    ),

    dim_months_clients as (

            select * from months
            cross join clients
    
    ),

    final as (

        --creating a undisrupted list of date and currencies
        select
            d.m as contract_month,
            d.client_id,
            c.spend_threshold,
            c.discounted_fee_margin
        from dim_months_clients as d
        inner join contracts as c on d.client_id = c.client_id
            and d.m between c.contract_start_date and c.contract_end_date

    )

select * from final
