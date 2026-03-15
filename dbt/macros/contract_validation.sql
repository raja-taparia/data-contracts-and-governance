{% macro _get_contract_columns(contract_path) %}
  {#
  Load the contract file and return an ordered list of column names defined under `schema.properties`.

  Usage:
      {% set cols = _get_contract_columns('contracts/finance/accounts/v1.yaml') %}
  #}

  {% set contract = load_yaml(contract_path) %}
  {% if contract is none %}
    {{ exceptions.raise_compiler_error("Unable to load contract YAML: '{}'".format(contract_path)) }}
  {% endif %}

  {% set props = contract.get('schema', {}).get('properties', {}) %}
  {% if props is none %}
    {{ exceptions.raise_compiler_error("Contract YAML does not contain 'schema.properties' at: '{}'".format(contract_path)) }}
  {% endif %}

  {{ return(props.keys() | list) }}
{% endmacro %}

{% test contract_schema_matches_model(model, contract_path) %}
  {#
  Validate that the compiled model has the same columns as the contract YAML.

  This test will fail if any columns are defined in the contract but missing from the compiled model.
  #}

  {% set contract_columns = _get_contract_columns(contract_path) %}
  {% set model_cols = adapter.get_columns_in_relation(ref(model)) | map(attribute='name') | list %}

  {% set missing = [] %}
  {% for col in contract_columns %}
    {% if col not in model_cols %}
      {% do missing.append(col) %}
    {% endif %}
  {% endfor %}

  {% if missing | length > 0 %}

    select '{{ missing[0] }}' as missing_column
    {% for col in missing[1:] %}
      union all
      select '{{ col }}'
    {% endfor %}

  {% else %}

    -- No missing columns; return empty result set
    select 1 where 1 = 0

  {% endif %}

{% endtest %}

{% test contract_schema_diff(model, contract_path) %}
  {#
  Report differences between the contract schema and the compiled model schema.

  - `missing`: column in contract but not in model
  - `extra`: column in model but not in contract

  Returns rows with (diff_type, column_name) for every difference.
  #}

  {% set contract_columns = _get_contract_columns(contract_path) %}
  {% set model_cols = adapter.get_columns_in_relation(ref(model)) | map(attribute='name') | list %}

  {% set missing = [] %}
  {% set extra = [] %}

  {% for col in contract_columns %}
    {% if col not in model_cols %}
      {% do missing.append(col) %}
    {% endif %}
  {% endfor %}

  {% for col in model_cols %}
    {% if col not in contract_columns %}
      {% do extra.append(col) %}
    {% endif %}
  {% endfor %}

  {% if missing | length > 0 or extra | length > 0 %}

    {% if missing | length > 0 %}
      select 'missing' as diff_type, '{{ missing[0] }}' as column_name
      {% for col in missing[1:] %}
        union all
        select 'missing', '{{ col }}'
      {% endfor %}
    {% endif %}

    {% if extra | length > 0 %}
      {% if missing | length > 0 %}
        union all
      {% endif %}
      select 'extra' as diff_type, '{{ extra[0] }}' as column_name
      {% for col in extra[1:] %}
        union all
        select 'extra', '{{ col }}'
      {% endfor %}
    {% endif %}

  {% else %}

    -- Schema matches contract perfectly; return empty result set
    select 1 where 1 = 0

  {% endif %}

{% endtest %}
