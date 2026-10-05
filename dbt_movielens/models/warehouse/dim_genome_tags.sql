with tags as (
    select *
    from {{ ref('stg_genome-tags') }}
)
select
    tagId as tag_id,
    trim(tag) as tag_name
from tags