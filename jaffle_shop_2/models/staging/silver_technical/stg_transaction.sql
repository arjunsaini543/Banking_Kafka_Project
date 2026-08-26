{{ config(materialized='view') }}

with ranked as (

    select
        ID as transaction_id,
        ACCOUNT_ID as account_id,
        TXN_TYPE as transaction_type,
        AMOUNT::FLOAT as amount,
        RELATED_ACCOUNT_ID as related_account_id,
        STATUS as status,
        CREATED_AT as created_at,
        current_timestamp() as load_timestamp,

        row_number() over (
            partition by ID
            order by CREATED_AT desc
        ) as rn

    from {{ source('raw', 'transactions') }}

    where ID is not null
      and ACCOUNT_ID is not null
      and TXN_TYPE is not null
      and AMOUNT is not null
      and STATUS is not null
      and CREATED_AT is not null
)

select
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    created_at,
    load_timestamp

from ranked

where rn = 1