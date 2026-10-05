FROM astrocrpublic.azurecr.io/runtime:3.3-8

# Cria o ambiente virtual isolado para o dbt com suporte ao DuckDB
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir "dbt-core==1.12.*" "dbt-duckdb==1.11.*" "duckdb>=1.5.3"

ENV PATH="${PATH}:/usr/local/airflow/dbt_venv/bin"