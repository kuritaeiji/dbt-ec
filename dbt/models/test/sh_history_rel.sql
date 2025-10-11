{# tests/save_history_incremental.sql で使用するモデル。 #}

SELECT
  '1' AS customer_hkey
  , '1' || 'A' AS customer_hdiff
  , '1' AS customer_code
  , 'A' AS customer_name
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
  , false AS deleted
UNION ALL
SELECT
  '2' AS customer_hkey
  , '2' || 'B' AS customer_hdiff
  , '2' AS customer_code
  , 'B' AS customer_name
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
  , false AS deleted
UNION ALL
SELECT
  '3' AS customer_hkey
  , '3' || 'C' AS customer_hdiff
  , '3' AS customer_code
  , 'C' AS customer_name
  , TIMESTAMP '2000-01-01 00:00:00' AS load_ts_utc
  , false AS deleted