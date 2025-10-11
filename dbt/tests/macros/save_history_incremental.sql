WITH actual AS (
  {{ save_history(
    input_rel=ref("sh_input_rel"),
    key_column='customer_hkey',
    diff_column='customer_hdiff',
    history_rel=ref('sh_history_rel'),
    force_incremental=True
  )}}
),

expected AS (
  SELECT
    '2' AS customer_hkey
    , '2' || 'BB' AS customer_hdiff
    , '2' AS customer_code
    , 'BB' AS customer_name
    , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
    , false AS deleted
  UNION ALL
  SELECT
    '3' AS customer_hkey
    , '3' || 'C' AS customer_hdiff
    , '3' AS customer_code
    , 'C' AS customer_name
    , TIMESTAMP '{{ run_started_at }}' AS load_ts_utc
    , true AS deleted
  UNION ALL
  SELECT
    '4' AS customer_hkey
    , '4' || 'D' AS customer_hdiff
    , '4' AS customer_code
    , 'D' AS customer_name
    , TIMESTAMP '2000-01-02 00:00:00' AS load_ts_utc
    , false AS deleted
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