{# tests/save_history_incremental.sql で使用するモデル。 #}

{# 1 は、そのまま #}
SELECT
  '1' AS customer_hkey
  , '1' || 'A' AS customer_hdiff
  , '1' AS customer_code
  , 'A' AS customer_name
  , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
UNION ALL
{# 2 は、更新 #}
SELECT
  '2' AS customer_hkey
  , '2' || 'BB' AS customer_hdiff
  , '2' AS customer_code
  , 'BB' AS customer_name
  , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
UNION ALL
{# 3 は、削除 #}
{# 4 は、新規作成 #}
SELECT
  '4' AS customer_hkey
  , '4' || 'D' AS customer_hdiff
  , '4' AS customer_code
  , 'D' AS customer_name
  , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc