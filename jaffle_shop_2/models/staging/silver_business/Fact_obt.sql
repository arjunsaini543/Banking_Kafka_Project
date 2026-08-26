{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT

    -- Transaction details
    t.transaction_id,
    t.account_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.created_at AS transaction_time,

    -- Account details
    a.customer_id,
    a.account_type,
    a.balance,
    a.currency,
    a.created_at AS account_created_at,

    -- Customer details
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.created_at AS customer_created_at,

    CURRENT_TIMESTAMP() AS load_timestamp

FROM {{ ref('stg_transaction') }} t

LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id

LEFT JOIN {{ ref('stg_customers') }} c
    ON a.customer_id = c.customer_id