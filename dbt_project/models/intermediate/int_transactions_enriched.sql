-- Takes cleaned staging transactions and adds derived fields that are useful across multiple mart models.
--
-- Derived fields computed here rather than in marts so the logic lives in one place — if the definition of "high value"
-- changes, we update it here and all marts get it automatically.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

enriched as (
    select
        txn_id,
        amount_inr,
        city,
        state,
        merchant_category,
        upi_app,
        payment_type,
        status,
        failure_reason,
        txn_date,
        txn_hour,
        is_weekend,
        is_festival,
        sender_upi,
        receiver_upi,
        device_type,
        created_at,

        -- derived: success flag as boolean and integer
        -- integer version is useful for SUM() to count successes
        case when status = 'SUCCESS' then true else false end as is_success,
        case when status = 'SUCCESS' then 1 else 0 end as is_success_int,

        -- derived: time of day bucket
        -- useful for Grafana heatmap panels without complex SQL in the panel
        case
            when txn_hour between 5  and 8  then 'Early Morning'
            when txn_hour between 9  and 11 then 'Morning'
            when txn_hour between 12 and 13 then 'Lunch'
            when txn_hour between 14 and 16 then 'Afternoon'
            when txn_hour between 17 and 19 then 'Evening'
            when txn_hour between 20 and 22 then 'Night'
            else 'Late Night'
        end as time_of_day,

        -- derived: transaction size bucket
        case
            when amount_inr < 100 then 'Micro (<₹100)'
            when amount_inr < 500 then 'Small (₹100-500)'
            when amount_inr < 2000 then 'Medium (₹500-2K)'
            when amount_inr < 10000 then 'Large (₹2K-10K)'
            else 'High Value (>₹10K)'
        end as amount_bucket,

        -- derived: month and year for partitioning in fact table
        extract(year  from txn_date)::int as txn_year,
        extract(month from txn_date)::int as txn_month,

        -- derived: day of week name
        to_char(txn_date, 'Day') as day_name

    from transactions
)

select * from enriched