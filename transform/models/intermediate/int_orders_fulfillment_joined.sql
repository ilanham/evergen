with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

stg_fulfillment as (
    select * from {{ ref('stg_fulfillment') }}
),

match_map as (
    select * from {{ ref('int_order_match_map') }}
),

joined as (
    select
        m.match_type,

        -- Order fields
        o.order_id,
        o.customer_code,
        o.customer_name,
        o.product_code,
        o.canonical_sku,
        o.product_description,
        o.ordered_qty,
        o.order_date,
        o.requested_ship_date,
        o.region,
        o.rep_id,

        -- Fulfillment fields
        f.fulfillment_id,
        f.raw_sku,
        f.shipped_qty,
        f.ship_date,
        f.carrier,
        f.delivery_confirmed,
        f.is_partial_shipment,
        f._is_duplicate,
        f.notes,

        -- Derived metrics
        coalesce(f.shipped_qty, 0)                              as fill_qty,
        case
            when o.ordered_qty is null or o.ordered_qty = 0 then null
            else coalesce(f.shipped_qty, 0) * 1.0 / o.ordered_qty
        end                                                     as fill_rate,
        coalesce(f.shipped_qty, 0) > coalesce(o.ordered_qty, 0) as is_overfilled
    from match_map m
    left join stg_orders o
        on o.order_id = m.order_id
    left join stg_fulfillment f
        on f.fulfillment_id = m.fulfillment_id
)

select * from joined
