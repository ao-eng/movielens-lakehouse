WITH source AS (
    SELECT * 
    FROM read_parquet({{ source("raw_movielens", "movies") }})
)

SELECT * FROM source