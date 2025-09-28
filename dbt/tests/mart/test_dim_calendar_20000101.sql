with actual as (
    select
        * EXCEPT (load_ts_utc)
    from {{ ref('dim_calendar') }}
    where calendar_code = '2000-01-01-00'
),
expected as (
    select
        '2000-01-01-00' as calendar_code,
        DATE '2000-01-01' as calendar_date,
        2000 as year,
        1 as quarter,
        1 as month,
        52 as week,
        7 as day_of_week,
        1 as day,
        10957 as diff_day_from_start_date,
        1565 as diff_week_from_start_date,
        360 as diff_month_from_start_date,
        120 as diff_quarter_from_start_date,
        30 as diff_year_from_start_date,
        0 as diff_day_from_year,
        0 as diff_week_from_year,
        0 as diff_month_from_year,
        0 as diff_quarter_from_year,
        0 as diff_day_from_quarter,
        0 as diff_week_from_quarter,
        0 as diff_month_from_quarter,
        0 as diff_day_from_month,
        0 as diff_week_from_month,
        false as is_business_day,
        'dbt' as record_source
)
(
    select * from expected
    except distinct
    select * from actual
)
union all
(
    select * from actual
    except distinct
    select * from expected
)
