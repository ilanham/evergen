with unmatched as (
    select * from {{ ref('int_orders_fulfillment_joined') }}
    where match_type = 'unmatched_fulfillment'
)

select
    fulfillment_id,
    raw_sku        as sku,
    canonical_sku,
    shipped_qty,
    shipped_qty    as volume_at_risk,
    ship_date,
    carrier,
    notes
from unmatched
