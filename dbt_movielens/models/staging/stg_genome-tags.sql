WITH source AS (
    SELECT * 
    FROM read_parquet({{ source("raw_movielens", "genome-tags") }})
)

SELECT * FROM source