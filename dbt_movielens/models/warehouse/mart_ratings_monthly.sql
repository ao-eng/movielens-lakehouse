select
    date_trunc('month', rating_at) as rating_month,
    count(*)                       as total_ratings,
    avg(rating)                    as avg_rating
from {{ ref('fct_ratings') }}
group by 1