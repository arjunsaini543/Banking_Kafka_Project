{{ config(materialized='view') }}

with ranked as (

    select
        ID as account_id,
        CUSTOMER_ID as customer_id,
        ACCOUNT_TYPE as account_type,
        BALANCE as balance,
        CURRENCY as currency,
        CREATED_AT as created_at,
        current_timestamp() as load_timestamp,

        row_number() over (
            partition by ID
            order by CREATED_AT desc
        ) as rn

    from {{ source('raw', 'accounts') }}

    where ID is not null
      and CUSTOMER_ID is not null
      and ACCOUNT_TYPE is not null
      and CURRENCY is not null
      and CREATED_AT is not null
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp

from ranked

where rn = 1
{# - name: account_id
  data_tests:
    - not_null
    - unique

- name: customer_id
  data_tests:
    - not_null #}