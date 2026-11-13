select 1 as failure
where (select sum(revenue) from {{ ref('mart_product_reviews') }})
   <> (select sum(amount) from {{ ref('fct_orders') }} where status='paid')
or (select sum(paid_orders) from {{ ref('mart_product_reviews') }})
   <> (select count(*) from {{ ref('fct_orders') }} where status='paid')
