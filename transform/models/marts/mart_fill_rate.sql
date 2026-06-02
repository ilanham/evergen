with joined as (
    select * from {{ ref('int_orders_fulfillment_joined') }}
    where match_type in ('direct', 'inferred')
),

aggregated as (
    select
        {{ dbt_utils.generate_surrogate_key(['canonical_sku', 'region']) }} as fill_rate_id,
        canonical_sku                           as product_code,
        region,
        sum(ordered_qty)                        as total_ordered_qty,
        sum(fill_qty)                           as total_shipped_qty,
        least(
            case
                when sum(ordered_qty) = 0 then 0.0
                else sum(fill_qty) * 1.0 / sum(ordered_qty)
            end,
            1.0
        )                                       as fill_rate_pct
    from joined
    group by canonical_sku, region
)

select * from aggregated
