WITH source AS (
    SELECT
        userId as user_id,
        movieId as movie_id,
        timestamp as rating_timestamp,
        rating,
        make_timestamp(timestamp::bigint * 1000000) as rating_at,
    FROM read_parquet({{ source("raw_movielens", "ratings") }})
)

SELECT * FROM source