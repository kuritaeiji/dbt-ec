{# with_end_ts_history.sql のテストに使用される #}
SELECT
  '1' AS customer_hkey
  , 'A' AS customer_hdiff
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
UNION ALL
SELECT
  '1' AS customer_hkey
  , 'AA' AS customer_hdiff
  , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
UNION ALL
SELECT
  '1' AS customer_hkey
  , 'AAA' AS customer_hdiff
  , TIMESTAMP '2000-01-03 00:00:00' AS load_ts_utc
UNION ALL
SELECT
  '2' AS customer_hkey
  , 'B' AS customer_hdiff
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
UNION ALL
SELECT
  '2' AS customer_hkey
  , 'B' AS customer_hdiff
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
UNION ALL
SELECT
  '2' AS customer_hkey
  , 'BB' AS customer_hdiff
  , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc