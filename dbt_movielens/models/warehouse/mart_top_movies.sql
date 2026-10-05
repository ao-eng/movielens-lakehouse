with movie_stats as (
    select
        movie_id,
        count(*)      as total_ratings,
        avg(rating)   as avg_rating
    from {{ ref('fct_ratings') }}
    group by 1
),

global_avg as (
    select avg(rating) as c from {{ ref('fct_ratings') }}
),

weighted as (
select
    m.movie_title,
    m.release_decade,
    s.total_ratings,
    s.avg_rating,
    (s.total_ratings / (s.total_ratings + 1000.0)) * s.avg_rating
      + (1000.0 / (s.total_ratings + 1000.0)) * g.c   as weighted_rating
from movie_stats s
join {{ ref('dim_movies') }} m using (movie_id)
cross join global_avg g
where m.release_decade is not null
)

select
    *,
    row_number() over (
        partition by release_decade
        order by weighted_rating desc
    ) as rank_in_decade
from weighted