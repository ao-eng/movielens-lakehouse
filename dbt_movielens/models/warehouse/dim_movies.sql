{{
    config(
        on_schema_change='append_new_columns'
    )
}}
with cte as (
    select *
    from {{ ref('stg_movies') }} as movies  
)
select
    cte.movieId as movie_id,
    trim(cte.title) as movie_title,
    split(cte.genres, '|') as genres_array,
    genres,
    try_cast(regexp_extract(movie_title, '\((\d{4})\)\s*$', 1) as integer) as release_year,
    (release_year // 10) * 10 as release_decade
from cte
