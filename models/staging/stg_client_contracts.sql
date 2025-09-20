with source as (
    
        select * from {{ ref('seed_client_contracts') }}

    ),

    final as (

        select
            client_id,
            contract_start_date,
            contract_duration_months,
            spend_threshold,
            discounted_fee_margin,
            --get end date of each contract
            date(contract_start_date,
                printf('+%d months', contract_duration_months), '-1 day') as contract_end_date
        from source

    )

select * from final
