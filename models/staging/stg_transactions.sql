with source as (

        select * from {{ ref('seed_transactions') }}
    
    ),

    final as (

        select
            transaction_id,
            client_id,
            transaction_amount,
            transaction_type,
            platform_fee_margin,
            currency,
            linked_transaction_id,
            date(transaction_date) as transaction_date,
            datetime(transaction_date, 'start of month') as transaction_month
        from source

    )

select * from final
