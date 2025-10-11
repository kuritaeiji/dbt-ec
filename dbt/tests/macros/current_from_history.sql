{# 重複排除、最新の履歴レコードのみ取得 #}
WITH actual AS (
  {{ current_from_history(
    ref('cfh_history_rel'),
    'customer_hkey',
    'load_ts_utc'
  )}}
),

expected AS (
  SELECT
    '1' AS customer_hkey
    , TIMESTAMP '2000-01-01 00:00:01' AS load_ts_utc
  UNION ALL
  SELECT
    '2' AS customer_hkey
    , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
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