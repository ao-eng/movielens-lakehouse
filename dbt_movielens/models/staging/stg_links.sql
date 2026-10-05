WITH source AS (
    SELECT * 
    FROM read_parquet({{ source("raw_movielens", "links") }})
)

SELECT * FROM source