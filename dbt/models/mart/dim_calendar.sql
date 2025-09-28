{% set start_date='1970-01-01' -%}
{% set end_date='2100-12-31' -%}

WITH days as (
  SELECT day as calendar_date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL 1 DAY)) AS day
),

days_with_additional_info as (
  SELECT
    calendar_date
    , EXTRACT(YEAR FROM calendar_date) as year
    , EXTRACT(QUARTER FROM calendar_date) as quarter
    , EXTRACT(MONTH FROM calendar_date) as month
    , EXTRACT(ISOWEEK FROM calendar_date) as week -- ISO 8601 基準の週番号
    , EXTRACT(DAYOFWEEK FROM calendar_date) as day_of_week -- (1=日曜, 2=月曜, ... 7=土曜)
    , EXTRACT(DAY FROM calendar_date) as day
  FROM days
),

standard_calendar as (
  SELECT
    CAST(CALENDAR_DATE AS STRING) || '-00' as calendar_code
    , calendar_date
    , year
    , quarter
    , month
    , week
    , day_of_week
    , day
    {# 全期間 #}
    , DATE_DIFF(calendar_date, DATE('{{ start_date }}'), DAY) as diff_day_from_start_date
    , DATE_DIFF(calendar_date, DATE('{{ start_date }}'), ISOWEEK) as diff_week_from_start_date
    , DATE_DIFF(calendar_date, DATE('{{ start_date }}'), MONTH) as diff_month_from_start_date
    , DATE_DIFF(calendar_date, DATE('{{ start_date }}'), QUARTER) as diff_quarter_from_start_date
    , DATE_DIFF(calendar_date, DATE('{{ start_date }}'), YEAR) as diff_year_from_start_date
    {# 年 #}
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, YEAR), DAY) as diff_day_from_year
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, YEAR), ISOWEEK) as diff_week_from_year
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, YEAR), MONTH) as diff_month_from_year
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, YEAR), QUARTER) as diff_quarter_from_year
    {# 四半期 #}
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, QUARTER), DAY) as diff_day_from_quarter
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, QUARTER), ISOWEEK) as diff_week_from_quarter
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, QUARTER), MONTH) as diff_month_from_quarter
    {# 月 #}
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, MONTH), DAY) as diff_day_from_month
    , DATE_DIFF(calendar_date, DATE_TRUNC(CALENDAR_DATE, MONTH), ISOWEEK) as diff_week_from_month
    {# 営業日 #}
    , CASE
      {# 月曜 ~ 金曜: 営業日 #}
      WHEN day_of_week BETWEEN 2 AND 6 THEN true
      ELSE false
    END as is_business_day
    , 'dbt' as record_source
    , '{{ run_started_at }}' as load_ts_utc
  FROM days_with_additional_info
),

default_record as (
    SELECT
      '-1' as calendar_code
      , DATE '0001-01-01' calendar_date
      , -1 as year
      , -1 as quarter
      , -1 as month
      , -1 as week
      , -1 as day_of_week
      , -1 as day
      , -1 as diff_day_from_start_date
      , -1 as diff_week_from_start_date
      , -1 as diff_month_from_start_date
      , -1 as diff_quarter_from_start_date
      , -1 as diff_year_from_start_date
      , -1 as diff_day_from_year
      , -1 as diff_week_from_year
      , -1 as diff_month_from_year
      , -1 as diff_quarter_from_year
      , -1 as diff_day_from_quarter
      , -1 as diff_week_from_quarter
      , -1 as diff_month_from_quarter
      , -1 as diff_day_from_month
      , -1 as diff_week_from_month
      , false as is_business_day
    , '{{ var('default_record_source_record') }}' as record_source
    , '{{ run_started_at }}' as load_ts_utc
),

with_default_record as (
  SELECT * FROM standard_calendar
  UNION ALL
  SELECT * FROM default_record
)

SELECT * FROM with_default_record