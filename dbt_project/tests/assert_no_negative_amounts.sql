-- Fails if any transaction has amount <= 0
-- Returns the bad rows — a passing test returns zero rows

select *
from {{ ref('fact_transactions') }}
where amount_inr <= 0