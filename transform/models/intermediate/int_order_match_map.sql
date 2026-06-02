-- Three-level match strategy linking fulfillment records to orders:
--   1. direct    — fulfillment.order_ref = orders.order_id
--   2. inferred  — null order_ref, unambiguous canonical_sku match to one unmatched order
--   3. unmatched — residuals on both sides

with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

stg_fulfillment as (
    select * from {{ ref('stg_fulfillment') }}
),

direct_matches as (
    select
        o.order_id,
        f.fulfillment_id,
        'direct' as match_type
    from stg_fulfillment f
    inner join stg_orders o
        on o.order_id = f.order_ref
),

-- For null order_ref records, attempt SKU-based inference.
-- Only accept when exactly one unmatched order shares the canonical_sku
-- to avoid spurious multi-match assignments.
inferred_candidates as (
    select
        f.fulfillment_id,
        f.canonical_sku,
        count(distinct o.order_id) as candidate_count
    from stg_fulfillment f
    inner join stg_orders o
        on o.canonical_sku = f.canonical_sku
    where f.order_ref is null
      and f.fulfillment_id not in (select fulfillment_id from direct_matches)
      and o.order_id not in (select order_id from direct_matches)
    group by f.fulfillment_id, f.canonical_sku
    having count(distinct o.order_id) = 1
),

inferred_matches as (
    select
        o.order_id,
        f.fulfillment_id,
        'inferred' as match_type
    from stg_fulfillment f
    inner join inferred_candidates ic
        on ic.fulfillment_id = f.fulfillment_id
    inner join stg_orders o
        on o.canonical_sku = f.canonical_sku
        and o.order_id not in (select order_id from direct_matches)
),

unmatched_orders as (
    select
        o.order_id,
        null::text as fulfillment_id,
        'unmatched_order' as match_type
    from stg_orders o
    where o.order_id not in (select order_id from direct_matches)
      and o.order_id not in (select order_id from inferred_matches)
),

unmatched_fulfillments as (
    select
        null::bigint as order_id,
        f.fulfillment_id,
        'unmatched_fulfillment' as match_type
    from stg_fulfillment f
    where f.fulfillment_id not in (select fulfillment_id from direct_matches)
      and f.fulfillment_id not in (select fulfillment_id from inferred_matches)
)

select * from direct_matches
union all
select * from inferred_matches
union all
select * from unmatched_orders
union all
select * from unmatched_fulfillments
