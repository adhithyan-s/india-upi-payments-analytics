{{
    config(materialized='table')
}}

with apps as (
    select app_name, company, market_share_pct from (values
        ('phonepe',    'Walmart',       48.0),
        ('google pay', 'Google',        37.0),
        ('paytm',      'One97 Comms',    8.0),
        ('bhim',       'NPCI',           4.0),
        ('amazon pay', 'Amazon',         3.0)
    ) as t(app_name, company, market_share_pct)
)

select
    {{ dbt_utils.generate_surrogate_key(['app_name']) }} as upi_app_sk,
    app_name,
    company,
    market_share_pct
from apps