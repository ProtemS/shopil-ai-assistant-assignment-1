with valid as (
    select cast(review_id as integer) review_id,
           cast(order_id as integer) order_id,
           cast(product__id as integer) product_id,
           cast(rating as integer) rating,
           trim(text) review_text,
           cast(created_at as timestamp) created_at,
           _dlt_id
    from {{ source('raw', 'reviews') }}
    where rating between 1 and 5 and nullif(trim(text), '') is not null
)
select review_id, order_id, product_id, rating, review_text, created_at
from valid
qualify row_number() over(partition by review_id order by created_at, review_text, rating, order_id, product_id) = 1
