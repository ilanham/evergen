with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_code,
        customer_name,
        product_code,
        {{ normalize_sku('product_code') }}           as canonical_sku,
        product_description,
        ordered_qty,
        {{ normalize_date('order_date') }}            as order_date,
        {{ normalize_date('requested_ship_date') }}   as requested_ship_date,
        coalesce(region, 'UNKNOWN')                   as region,
        rep_id
    from source
)

select * from renamed
