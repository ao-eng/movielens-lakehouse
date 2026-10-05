select
    lower(trim(tag))            as tag,
    count(*)                    as times_applied,
    count(distinct movie_id)    as movies_tagged,
    count(distinct user_id)     as users_tagging
from {{ ref('snap_tags') }}
where dbt_valid_to is null
group by 1