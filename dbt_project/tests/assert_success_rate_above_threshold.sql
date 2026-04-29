-- UPI success rate should be above 85% overall.
-- If it drops below that, something is wrong with the data.
-- Returns one row if the test fails, zero rows if it passes.

with stats as (
    select
        count(*) as total,
        sum(is_success_int) as successful
    from {{ ref('fact_transactions') }}
)

select *
from stats
where (successful * 100.0 / nullif(total, 0)) < 85