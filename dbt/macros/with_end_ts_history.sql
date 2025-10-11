{% macro with_end_ts_history(
  history_rel,
  key_column,
  diff_column,
  load_ts_column='load_ts_utc'
) %}

WITH unique_history AS (
  SELECT
    *
  FROM
    {{ history_rel }}
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY {{ key_column }}, {{ diff_column }}, {{ load_ts_column }}
      ORDER BY {{ load_ts_column }} ASC
    ) = 1
),

add_end_ts AS (
  SELECT
    *
    , LEAD({{ load_ts_column }}, 1, CAST('9999-12-31 23:59:59.999999' AS TIMESTAMP)) OVER (
        PARTITION BY {{ key_column }} ORDER BY {{ load_ts_column }} ASC
      ) AS end_ts_utc
  FROM unique_history
)

SELECT *
FROM add_end_ts
{% endmacro %}
