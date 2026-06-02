with source as (
    select * from {{ source('raw', 'fulfillment') }}
),

-- Flag rows that share a fulfillment_id before deduplication so the surviving
-- row carries the _is_duplicate marker (F-2013 reship case).
flagged as (
    select
        *,
        count(*) over (partition by fulfillment_id) > 1 as _is_duplicate
    from source
),

-- Keep the latest record per fulfillment_id (most recent ship_date wins).
deduped as (
    select *
    from flagged
    qualify row_number() over (
        partition by fulfillment_id
        order by ship_date desc
    ) = 1
),

renamed as (
    select
        fulfillment_id,
        try_cast(order_ref as bigint)      as order_ref,
        account_id,
        sku                                as raw_sku,
        {{ normalize_sku('sku') }}         as canonical_sku,
        shipped_qty,
        ship_date::date                    as ship_date,
        carrier,
        delivery_confirmed,
        notes,
        _is_duplicate,
        lower(notes) like '%partial%'      as is_partial_shipment
    from deduped
)

select * from renamed
