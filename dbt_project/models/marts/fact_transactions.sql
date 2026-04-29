-- Central fact table of the star schema.
-- One row per UPI transaction.
-- Grain: one UPI transaction identified by txn_id.
--
-- IMP: incremental materialisation
-- On first run: processes all rows from int_transactions_enriched
-- On subsequent runs: only processes rows where created_at is newer than the latest row already in the table.
-- This avoids rebuilding 180K+ rows on every dbt run.

{{
    config(
        materialized='incremental',
        unique_key='txn_sk',
        on_schema_change='sync_all_columns'
    )
}}

with transactions as (
    select * from {{ ref('int_transactions_enriched') }}

    -- IMP: is_incremental() block
    -- Only runs on runs after the first — filters to new rows only.
    -- The 1-hour lookback catches any late-arriving records that might have loaded between runs.
    {% if is_incremental() %}
    where created_at > (
        select max(created_at) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
),

dim_location as (
    select location_sk, city from {{ ref('dim_location') }}
),

dim_date as (
    select date_sk, date_day from {{ ref('dim_date') }}
),

dim_merchant as (
    select merchant_sk, category_code from {{ ref('dim_merchant') }}
),

dim_upi_app as (
    select upi_app_sk, app_name from {{ ref('dim_upi_app') }}
),

dim_payment_type as (
    select payment_type_sk, type_code from {{ ref('dim_payment_type') }}
),

final as (
    select
        -- surrogate key for the fact table
        {{ dbt_utils.generate_surrogate_key(['t.txn_id']) }} as txn_sk,

        -- natural key kept for traceability back to source
        t.txn_id,

        -- foreign keys to dimension tables
        d.date_sk,
        l.location_sk,
        m.merchant_sk,
        u.upi_app_sk,
        p.payment_type_sk,

        -- measures
        t.amount_inr,
        t.is_success,
        t.is_success_int,

        -- useful attributes kept on fact for Grafana filtering
        -- these are low-cardinality so denormalising is acceptable
        t.status,
        t.time_of_day,
        t.amount_bucket,
        t.is_weekend,
        t.is_festival,
        t.device_type,
        t.txn_hour,
        t.txn_year,
        t.txn_month,

        -- audit fields
        t.created_at

    from transactions t
    left join dim_location     l on t.city             = l.city
    left join dim_date         d on t.txn_date          = d.date_day
    left join dim_merchant     m on t.merchant_category = m.category_code
    left join dim_upi_app      u on t.upi_app           = u.app_name
    left join dim_payment_type p on t.payment_type      = p.type_code
)

select * from final