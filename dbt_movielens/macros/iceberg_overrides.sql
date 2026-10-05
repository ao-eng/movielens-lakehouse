{#
  Ajustes do dbt-duckdb para o catálogo Iceberg no AWS Glue.
  Só afetam relações cujo database é var('iceberg_catalog'). O resto segue o padrão.
#}

{#
  1) CREATE TABLE com 'location'.
  O Glue exige location na criação de tabelas Iceberg e não herda o LocationUri do database.
  O invocation_id deixa o path único por execução, para uma tabela recriada
  não dividir a pasta com a versão anterior (o DROP não apaga os arquivos no Glue).
#}
{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {%- if language == 'sql' and not temporary and relation.database == var('iceberg_catalog') -%}
    {%- set location = var('iceberg_warehouse_path') ~ '/' ~ model['alias'] ~ '/' ~ invocation_id -%}
    create table {{ relation }}
    with ('location' = '{{ location }}')
    as (
      {{ compiled_code }}
    );
  {%- else -%}
    {{ return(dbt.duckdb__create_table_as(temporary, relation, compiled_code, language, partitioned_by, sorted_by)) }}
  {%- endif -%}
{%- endmacro %}

{#
  2) DROP sem CASCADE.
  O DuckDB ainda não suporta DROP TABLE ... CASCADE em tabelas Iceberg.
#}
{% macro duckdb__drop_relation(relation) -%}
  {%- if relation.database == var('iceberg_catalog') -%}
    {% call statement('drop_relation', auto_begin=False) -%}
      drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
  {%- else -%}
    {{ return(dbt.duckdb__drop_relation(relation)) }}
  {%- endif -%}
{% endmacro %}