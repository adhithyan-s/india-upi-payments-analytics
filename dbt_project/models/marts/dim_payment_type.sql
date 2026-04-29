{{
    config(materialized='table')
}}

with payment_types as (
    select type_code, type_name from (values
        ('p2m',          'Peer to Merchant'),
        ('p2p',          'Peer to Peer'),
        ('bill_payment', 'Bill Payment'),
        ('recharge',     'Mobile Recharge')
    ) as t(type_code, type_name)
)

select
    {{ dbt_utils.generate_surrogate_key(['type_code']) }} as payment_type_sk,
    type_code,
    type_name
from payment_types