with inbound_transactions as (
    
        select
            transaction_month,
            client_id,
            monthly_final_transacted_amount_gbp,
            actual_monthly_platform_fee_margin,
            original_monthly_platform_fee_margin,
            contracted_monthly_spend_threshold,
            monthly_spend_threshold_reached,
            monthly_final_transacted_amount_gbp 
                * original_monthly_platform_fee_margin as revenue_gbp_pre_discount,
            monthly_final_transacted_amount_gbp 
                * actual_monthly_platform_fee_margin as revenue_gbp_post_discount,
            monthly_final_transacted_amount_gbp 
                * (original_monthly_platform_fee_margin - actual_monthly_platform_fee_margin) as discount_applided_gbp,
            count(distinct transaction_id) as total_transaction_count,
            sum(transaction_amount_gbp) as total_transaction_amount_gbp,
            count(distinct case when is_refunded then transaction_id end) as total_refunded_transaction_count,
            sum(refund_amount_gbp) as total_refunded_amount_gbp
        from {{ ref('int_inbound_transactions') }}
        group by client_id, 
            transaction_month,
            monthly_spend_threshold_reached,
            monthly_final_transacted_amount_gbp,
            actual_monthly_platform_fee_margin,
            original_monthly_platform_fee_margin

    ),

    final as (

        select
            transaction_month,
            client_id,
            total_transaction_count,
            total_transaction_amount_gbp,
            total_refunded_transaction_count,
            total_refunded_amount_gbp,            
            actual_monthly_platform_fee_margin,
            original_monthly_platform_fee_margin,
            contracted_monthly_spend_threshold,
            monthly_spend_threshold_reached,
            revenue_gbp_pre_discount,
            revenue_gbp_post_discount,
            discount_applided_gbp,
            monthly_final_transacted_amount_gbp as monthly_gmv_gbp
        from inbound_transactions

    )

select * from final
