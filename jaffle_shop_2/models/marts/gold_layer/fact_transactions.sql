{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT
    transaction_id,
    account_id,
    customer_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    transaction_time,
    CURRENT_TIMESTAMP() AS load_timestamp

FROM {{ ref('Fact_obt') }}

{% if is_incremental() %}

WHERE transaction_time > (
    SELECT MAX(transaction_time)
    FROM {{ this }}
)

{% endif %}