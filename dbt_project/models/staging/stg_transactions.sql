-- Reads from raw.transactions and applies basic cleaning:
-- type casting, standardisation, deduplication.
--
-- This is NOT where business logic lives — just making data safe and consistent before the intermediate layer joins things together.
--
-- IMP: deduplication using ROW_NUMBER()
-- Raw layer may have duplicate txn_ids if the loader ran more than once before truncation, or from at-least-once delivery semantics.
-- We keep the latest version of each transaction by created_at.

with source as (
    select * from {{ source('raw', 'transactions') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by txn_id
            order by created_at desc
        ) as rn
    from source
),

cleaned as (
    select
        txn_id,
        cast(amount_inr as numeric(12, 2)) as amount_inr,
        upper(trim(city)) as city,
        upper(trim(state)) as state,
        lower(trim(merchant_category)) as merchant_category,
        lower(trim(upi_app)) as upi_app,
        lower(trim(payment_type)) as payment_type,
        upper(trim(status)) as status,
        failure_reason,
        txn_date,
        cast(txn_hour as smallint) as txn_hour,
        is_weekend,
        is_festival,
        sender_upi,
        receiver_upi,
        lower(trim(device_type)) as device_type,
        created_at,
        loaded_at
    from deduplicated
    where rn = 1
      and amount_inr > 0
      and txn_id is not null
)

select * from cleaned