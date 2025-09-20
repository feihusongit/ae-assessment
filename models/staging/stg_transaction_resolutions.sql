with source as (
    
        select * from {{ ref('seed_transaction_resolutions') }}

    ),

    final as (

        select
            transaction_id,
            resolution_status,
            resolution_date       
        from source

    )

select * from final
