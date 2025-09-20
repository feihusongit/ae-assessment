with recursive dates(rate_date) as (

        select date('2024-01-01')
        union all
        select date(rate_date, '+1 day') from dates
        where rate_date < date('2024-06-30')

    ),

    currencies as (

        select distinct currency from {{ ref('stg_currency_rates') }}

    ),

    dim_date_currencies as (

            select * from dates
            cross join currencies
    
    ),

    final as (

        --creating a undisrupted list of date and currencies
        select
            d.rate_date,
            d.currency,
            c.exchange_rate_to_gbp
        from dim_date_currencies as d
        left join {{ ref('stg_currency_rates') }} as c on d.currency = c.currency
            and d.rate_date = c.rate_date

    )

select * from final
