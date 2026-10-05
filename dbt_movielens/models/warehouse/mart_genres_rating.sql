with movie_genres as (
    select
        movie_id,
        unnest(string_split(genres, '|')) as genre
    from {{ ref('dim_movies') }}
)

select
    g.genre,
    count(*)      as total_ratings,
    avg(r.rating) as avg_rating
from {{ ref('fct_ratings') }} r
join movie_genres g using (movie_id)
where g.genre <> '(no genres listed)'
group by 1