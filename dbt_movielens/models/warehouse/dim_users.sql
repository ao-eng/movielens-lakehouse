with ratings as (
    select distinct
        user_id,
    from {{ ref('stg_ratings') }}
),
tags as (
    select distinct
        user_id,
    from {{ ref('stg_tags') }}
)
select user_id from (
    select user_id from ratings
    union
    select user_id from tags
) as users