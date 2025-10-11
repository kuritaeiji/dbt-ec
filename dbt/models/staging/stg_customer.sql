WITH src_data AS (
  SELECT
    id AS customer_code
    , name AS customer_name
    , TIMESTAMP '{{ run_started_at }}' AS load_ts_utc
    , 'ec.customer' AS record_source
  FROM
    {{ source('source', 'customer') }}
),
hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_code']) }} AS customer_hkey
    , {{ dbt_utils.generate_surrogate_key(['customer_code', 'customer_name']) }} AS customer_hdiff
    , *
  FROM src_data
)
SELECT *
FROM hashed