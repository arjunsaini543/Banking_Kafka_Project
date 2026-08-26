{{ config(materialized='view') }}

with ranked as (

    select
        ID as customer_id,
        FIRST_NAME as first_name,
        LAST_NAME as last_name,
        EMAIL as email,
        CREATED_AT as created_at,
        current_timestamp() as load_timestamp,

        row_number() over (
            partition by ID
            order by CREATED_AT desc
        ) as rn

    from {{ source('raw', 'customers') }}

    where ID is not null
      and FIRST_NAME is not null
      and LAST_NAME is not null
      and EMAIL is not null
      and CREATED_AT is not null
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp

from ranked

where rn = 1