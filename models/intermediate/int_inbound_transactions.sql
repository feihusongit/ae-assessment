with transaction_raw as (

        select
            client_id,
            transaction_id,
            transaction_date,
            transaction_month
            transaction_amount,
            transaction_type,
            platform_fee_margin,
            currency,
            linked_transaction_id
        from {{ ref('stg_transactions') }}

    ),

    chargeback_status as (

        select
            transaction_id,
            resolution_status,
            resolution_date
        from {{ ref('stg_transaction_resolutions') }}

    ),

    rate_to_gbp as (

        select
            rate_date,
            currency,
            exchange_rate_to_gbp
        from {{ ref('int_daily_currency_rate') }}

    ),


    client_contract_list as (

        select *
        from {{ ref('int_monthly_client_contracts') }}

    ),

    --single out refund transactions
    refunds as (

        select distinct
            linked_transaction_id,
            --transactions can sometime be refunded twice due to data issue
            --use data of last refund of each transaction
            first_value(currency)
                over (partition by linked_transaction_id
                    order by transaction_date desc) as currency,
            first_value(transaction_amount)
                over (partition by linked_transaction_id
                    order by transaction_date desc) as transaction_amount,
            first_value(transaction_date)
                over (partition by linked_transaction_id
                    order by transaction_date desc) as transaction_date
        from transaction_raw
        where linked_transaction_id is not null
            and transaction_type = 'refund'

    ),

    status_update as (
        
        select
            t_original.client_id,
            t_original.transaction_id,
            t_original.transaction_date,
            t_original.currency,
            fx_original.exchange_rate_to_gbp,
            t_original.transaction_month,
            t_original.transaction_type,
            t_original.transaction_amount as transaction_amount_ccy,
            t_original.transaction_amount * fx_original.exchange_rate_to_gbp as transaction_amount_gbp,
            t_original.platform_fee_margin,
            --refund
            coalesce(t_refund.linked_transaction_id is not null, false) as is_refunded,
            t_refund.transaction_date as refunded_date,
            t_refund.currency as refund_currency,
            t_refund.transaction_amount as refund_amount_local_ccy,
            t_refund.transaction_amount * fx_refund.exchange_rate_to_gbp as refund_amount_gbp,
            --chargeback
            ct.resolution_status as chargeback_resolution_status,
            ct.resolution_date as chargeback_resolution_date,
            --for refund, set transaction amount as 0. In case refund was done on different currency, do fx 
            --for fraud, set transaction amount to 0
            --for pending chargebacks, set transaction amount to 0
            case when t_refund.linked_transaction_id is not null
                    and t_refund.currency = t_original.currency
                    then t_refund.transaction_amount - t_original.transaction_amount
                when t_refund.linked_transaction_id is not null
                    and t_refund.currency != t_original.currency
                    then (t_refund.transaction_amount * fx_refund.exchange_rate_to_gbp / fx_original.exchange_rate_to_gbp)
                        - t_original.transaction_amount
                when t_original.transaction_type = 'fraud' then 0
                when ct.resolution_status = 'pending' then 0
                else t_original.transaction_amount
            end as final_transacted_amount_ccy
        from transaction_raw as t_original
        left join refunds as t_refund on t_refund.linked_transaction_id = t_original.transaction_id
        left join chargeback_status as ct on ct.transaction_id = t_original.transaction_id
        left join rate_to_gbp as fx_original on fx_original.currency = t_original.currency
            and fx_original.rate_date = t_original.transaction_date
        left join rate_to_gbp as fx_refund on fx_refund.currency = t_refund.currency
            and fx_refund.rate_date = t_refund.transaction_date
        --remove refund transactions as they have been joined to original transactions
        where t_original.linked_transaction_id is null

    ),

    add_monthly_revenue as (
        
        --calculate gross transacted amount + total monthly transacted amount
        select
            *,
            final_transacted_amount_ccy * exchange_rate_to_gbp as final_transacted_amount_gbp,
            sum(final_transacted_amount_ccy * exchange_rate_to_gbp)
                over (partition by client_id, transaction_month) as monthly_final_transacted_amount_gbp
        from status_update
    
    ),

    final as (

        select
            a.client_id,
            a.transaction_id,
            a.transaction_date,
            a.currency,
            a.exchange_rate_to_gbp,
            a.transaction_month,
            a.transaction_type,
            a.transaction_amount_ccy,
            a.transaction_amount_gbp,
            a.is_refunded,
            a.refunded_date,
            a.refund_currency,
            a.refund_amount_local_ccy,
            a.refund_amount_gbp,
            a.chargeback_resolution_status,
            a.chargeback_resolution_date,
            a.final_transacted_amount_ccy,
            a.final_transacted_amount_gbp,
            a.monthly_final_transacted_amount_gbp,
            c.spend_threshold as contracted_monthly_spend_threshold,
            a.platform_fee_margin as original_monthly_platform_fee_margin,
            case when c.spend_threshold <= a.monthly_final_transacted_amount_gbp then c.discounted_fee_margin
                else a.platform_fee_margin end as actual_monthly_platform_fee_margin,
            coalesce(c.spend_threshold <= a.monthly_final_transacted_amount_gbp, false) as monthly_spend_threshold_reached
        from add_monthly_revenue as a
        --looking at whether on client month level, the contracted spend threshold were met
        left join client_contract_list as c on c.client_id = a.client_id
            and a.transaction_month = c.contract_month

    )

select * from final
