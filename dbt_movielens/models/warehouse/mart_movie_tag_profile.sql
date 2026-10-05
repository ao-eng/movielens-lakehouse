select
    m.movie_title,
    m.movie_id,
    max(case when t.tag_name = 'dark'         then g.relevance end) as dark,
    max(case when t.tag_name = 'funny'        then g.relevance end) as funny,
    max(case when t.tag_name = 'violence'     then g.relevance end) as violence,
    max(case when t.tag_name = 'romantic'     then g.relevance end) as romantic,
    max(case when t.tag_name = 'mind-bending' then g.relevance end) as mind_bending
    -- mais 3 a 5 tags
from {{ ref('fct_genome_scores') }} g
join {{ ref('dim_genome_tags') }} t using (tag_id)
join {{ ref('dim_movies') }} m using (movie_id)
group by 1,2