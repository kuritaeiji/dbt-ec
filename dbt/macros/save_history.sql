{% macro save_history(
  input_rel,
  key_column,
  diff_column,
  history_rel=this,
  load_ts_column='load_ts_utc',
  force_incremental=False
) %}

{{ config(materialized = 'incremental') }}

WITH
{% if is_incremental() or force_incremental %}

current_from_history AS (
  {{ current_from_history(
    history_rel,
    key_column,
    load_ts_column
  )}}
),

load_from_input AS (
  SELECT
    i.* EXCEPT ({{ load_ts_column }})
    , i.{{ load_ts_column }} AS {{ load_ts_column }}
    , false AS deleted
  FROM {{ input_rel }} AS i
  LEFT OUTER JOIN current_from_history AS h
  ON i.{{ diff_column }} = h.{{ diff_column }}
    AND NOT h.deleted
  WHERE
    h.{{ diff_column }} IS NULL
),

load_from_deleted AS (
  SELECT
    h.* EXCEPT(deleted, {{ load_ts_column }})
    , TIMESTAMP '{{ run_started_at }}' AS {{ load_ts_column }}
    , true AS deleted
  FROM current_from_history AS h
  LEFT OUTER JOIN {{ input_rel }} AS i
  ON h.{{ key_column }} = i.{{ key_column }}
  WHERE
    i.{{ key_column }} IS NULL
    AND NOT h.deleted
),

changes_to_store AS (
  SELECT * FROM load_from_input
  UNION ALL
  SELECT * FROM load_from_deleted
)

{% else %}
changes_to_store AS (
  SELECT
    *
    , false AS deleted
  FROM {{ input_rel }}
)
{% endif %}

SELECT *
FROM changes_to_store
{% endmacro %}