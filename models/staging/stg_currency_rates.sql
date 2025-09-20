with source as (
    
        select * from {{ ref('seed_currency_rates') }}

    ),

    final as (
        
        select
            currency,
            rate_date,
            exchange_rate_to_gbp
        from source

    )

select * from final
