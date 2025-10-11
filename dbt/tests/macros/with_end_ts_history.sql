{# 重複排除、[適用開始日, 適用終了日) が追加される #}
WITH actual AS (
  {{
    with_end_ts_history(
      ref("weth_history_rel"),
      'customer_hkey',
      'customer_hdiff',
    )
  }}
),

expected AS (
  SELECT
  '1' AS customer_hkey
  , 'A' AS customer_hdiff
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
  , TIMESTAMP '2000-01-02 00:00:00' AS end_ts_utc
  UNION ALL
  SELECT
    '1' AS customer_hkey
    , 'AA' AS customer_hdiff
    , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
    , TIMESTAMP '2000-01-03 00:00:00' AS end_ts_utc
  UNION ALL
  SELECT
    '1' AS customer_hkey
    , 'AAA' AS customer_hdiff
    , TIMESTAMP '2000-01-03 00:00:00' AS load_ts_utc
    , TIMESTAMP '9999-12-31 23:59:59.999999' AS end_ts_utc
  UNION ALL
  SELECT
    '2' AS customer_hkey
    , 'B' AS customer_hdiff
    , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
    , TIMESTAMP '2000-01-02 00:00:00' AS end_ts_utc
  UNION ALL
  SELECT
    '2' AS customer_hkey
    , 'BB' AS customer_hdiff
    , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
    , TIMESTAMP '9999-12-31 23:59:59.999999' AS end_ts_utc
),

validation_errors AS (
  (
    SELECT * FROM actual
    EXCEPT DISTINCT
    SELECT * FROM expected
  )
  UNION ALL
    (
    SELECT * FROM expected
    EXCEPT DISTINCT
    SELECT * FROM actual
  )
)

SELECT * FROM validation_errors