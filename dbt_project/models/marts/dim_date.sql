-- Calendar dimension table covering the full data range.
-- Generated using dbt_utils.date_spine — no source table needed.
--
-- IMP: dim_date is always generated, never loaded from a source. 
-- It's built from a date spine — a complete sequence of every date in the range — so every possible date
-- exists as a row even if no transactions occurred that day.
-- This prevents gaps in time-series Grafana panels.

{{
    config(materialized='table')
}}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('2023-01-01' as date)",
            end_date = "cast('2025-01-01' as date)"
        )
    }}
),

-- festival dates hardcoded as a CTE
-- in production this would come from a seed file or source table
festival_dates as (
    select festival_date, festival_name from (values
        ('2023-01-14'::date, 'Makar Sankranti'),
        ('2023-01-26'::date, 'Republic Day'),
        ('2023-03-08'::date, 'Holi'),
        ('2023-08-15'::date, 'Independence Day'),
        ('2023-08-30'::date, 'Raksha Bandhan'),
        ('2023-09-19'::date, 'Ganesh Chaturthi'),
        ('2023-10-24'::date, 'Dussehra'),
        ('2023-11-12'::date, 'Diwali'),
        ('2023-11-13'::date, 'Diwali Day 2'),
        ('2023-12-25'::date, 'Christmas'),
        ('2023-12-31'::date, 'New Year Eve'),
        ('2024-01-01'::date, 'New Year'),
        ('2024-01-22'::date, 'Ram Mandir Consecration'),
        ('2024-01-26'::date, 'Republic Day'),
        ('2024-03-25'::date, 'Holi'),
        ('2024-08-15'::date, 'Independence Day'),
        ('2024-08-19'::date, 'Raksha Bandhan'),
        ('2024-09-07'::date, 'Ganesh Chaturthi'),
        ('2024-10-02'::date, 'Gandhi Jayanti'),
        ('2024-10-12'::date, 'Dussehra'),
        ('2024-10-31'::date, 'Diwali'),
        ('2024-11-01'::date, 'Diwali Day 2'),
        ('2024-12-25'::date, 'Christmas'),
        ('2024-12-31'::date, 'New Year Eve')
    ) as t(festival_date, festival_name)
),

final as (
    select
        -- surrogate key: integer YYYYMMDD format
        -- fast to join, human readable when debugging
        cast(to_char(date_day, 'YYYYMMDD') as int)  as date_sk,
        date_day,
        extract(year  from date_day)::int as year,
        extract(month from date_day)::int as month_num,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_short,
        extract(day from date_day)::int as day_of_month,
        extract(dow from date_day)::int as day_of_week_num,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_short,
        extract(quarter from date_day)::int as quarter,
        extract(week from date_day)::int as week_of_year,
        extract(doy from date_day)::int as day_of_year,

        -- weekend flag
        case when extract(dow from date_day) in (0, 6)
             then true else false end                 as is_weekend,

        -- month start/end flags useful for reporting
        case when extract(day from date_day) = 1
             then true else false end as is_month_start,
        case when date_day = date_trunc('month', date_day) + interval '1 month' - interval '1 day'
             then true else false end as is_month_end,

        -- Indian season (based on typical Indian climate calendar)
        case
            when extract(month from date_day) in (3, 4, 5) then 'Summer'
            when extract(month from date_day) in (6, 7, 8, 9) then 'Monsoon'
            when extract(month from date_day) in (10, 11) then 'Autumn'
            else 'Winter'
        end as season,

        -- fiscal quarter (India FY: April to March)
        case
            when extract(month from date_day) between 4 and 6  then 'Q1'
            when extract(month from date_day) between 7 and 9  then 'Q2'
            when extract(month from date_day) between 10 and 12 then 'Q3'
            else 'Q4'
        end as fiscal_quarter,

        -- festival fields
        coalesce(f.festival_name is not null, false)  as is_festival_day,
        f.festival_name

    from date_spine
    left join festival_dates f on date_day = f.festival_date
)

select * from final