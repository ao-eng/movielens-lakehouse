{#
  Materialização 'table' para catálogos Iceberg que não suportam RENAME (ex.: AWS Glue).
  Em vez de criar uma tabela temporária e renomear, faz DROP + CREATE da tabela alvo.
  Não é atômico: durante alguns segundos a tabela não existe. Aceitável para portfólio.
#}
{% materialization iceberg_table, adapter='duckdb', supported_languages=['sql'] %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if existing_relation is not none %}
    {{ adapter.drop_relation(existing_relation) }}
    {{ adapter.commit() }}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, compiled_code) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}