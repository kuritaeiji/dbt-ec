{% macro current_from_history(
  history_rel,
  key_column,
  load_ts_column='load_ts_utc',
) %}
  SELECT
    *
  FROM
    {{ history_rel }}
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY {{ key_column }} ORDER BY {{ load_ts_column }} DESC) = 1
{% endmacro %}