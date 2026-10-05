{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

with ratings as (
    select *
    from {{ ref('stg_ratings') }}
)
select 
    user_id,
    movie_id,
    rating,
    rating_timestamp,
    rating_at
from ratings
where rating is not null

{% if is_incremental() %}
    and rating_timestamp > (select max(rating_timestamp) from {{ this }})
{% endif %}