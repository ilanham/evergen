with unmatched as (
    select * from {{ ref('int_orders_fulfillment_joined') }}
    where match_type = 'unmatched_order'
)

select
    order_id,
    customer_code,
    customer_name,
    product_code,
    canonical_sku,
    region,
    ordered_qty,
    ordered_qty    as qty_at_risk,
    order_date,
    requested_ship_date,
    rep_id
from unmatched
