with genome_scores as (
    select *
    from {{ ref('stg_genome-scores') }}
)
select
    movieId as movie_id,
    tagId as tag_id,
    round(relevance, 4) as relevance
from genome_scores
where relevance > 0