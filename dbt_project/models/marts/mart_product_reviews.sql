with reviews as (
    select product_id, count(*) review_count, round(avg(rating),2) avg_rating,
           count(*) filter(where rating <= 2) negative_reviews
    from {{ ref('stg_reviews') }} group by product_id
), paid as (
    select product_id, count(*) paid_orders, sum(amount) revenue
    from {{ ref('fct_orders') }} where status='paid' group by product_id
)
select p.product_id, p.product_name, p.category,
       coalesce(r.review_count,0) review_count, r.avg_rating,
       coalesce(r.negative_reviews,0) negative_reviews,
       coalesce(s.paid_orders,0) paid_orders, coalesce(s.revenue,0) revenue
from {{ ref('stg_products') }} p
left join reviews r using(product_id)
left join paid s using(product_id)
