WITH source AS (
    select
    userId          as user_id,
    movieId         as movie_id,
    tag,
    to_timestamp(timestamp) as tag_timestamp
    FROM read_parquet({{ source("raw_movielens", "tags") }})
)

SELECT * FROM source