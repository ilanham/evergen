---
name: data-modeling
description: Use this skill whenever making decisions about data model design, entity relationships, join strategies, surrogate keys, grain definitions, slowly changing dimensions, or analytical mart structure for the Evergen pipeline. Trigger on any question about how to model orders vs fulfillment, how to handle the SKU mismatch, how to define fill rate, what the grain of a mart table should be, or how to handle unmatched records.
---

# Data Modeling — Evergen Data Platform

This skill covers the modeling decisions specific to Evergen's order-fulfillment domain. The core challenge is joining two siloed source systems that were never formally integrated.

## Entity Overview

### Orders (Source 1)
- **Primary key**: `order_id` (integer, assigned by Order Management System)
- **Grain**: one row per order line (one product per order per customer)
- **Customer identifier**: `customer_code` (e.g., `CUST-001`)
- **Product identifier**: `product_code` (e.g., `TIS-001-GF`, hyphen-delimited)

### Fulfillment (Source 2)
- **Primary key**: `fulfillment_id` (text, prefixed `F-`, assigned by Warehouse System)
- **Grain**: one row per shipment event (one SKU per shipment)
- **Customer identifier**: `account_id` (e.g., `ACC-101`) — **different namespace than `customer_code`**
- **Product identifier**: `sku` (e.g., `TIS001GF`, no hyphens) — **different format than `product_code`**
- **Order linkage**: `order_ref` field intended to reference `order_id`, but is nullable and not enforced

## The Join Problem

The two systems share no enforced foreign key. The join strategy must be layered:

1. **Primary join**: `fulfillment.order_ref = orders.order_id` (direct reference). Use this when `order_ref` is not null.
2. **Fallback join**: When `order_ref` is null, attempt to infer a match using `canonical_sku` (normalized SKU) + ship date proximity to `requested_ship_date`. Flag these as `match_type = 'inferred'`.
3. **Unmatched orders**: Orders with no fulfillment record after both join attempts → `match_type = 'unmatched_order'`.
4. **Unmatched fulfillments**: Fulfillment records with no matching order → `match_type = 'unmatched_fulfillment'`.

Document the match logic in `int_order_match_map`. Do not bury join logic in mart models.

## SKU Normalization

The canonical SKU key is produced by stripping hyphens and uppercasing:
- `TIS-001-GF` → `TIS001GF`
- `TIS001GF` → `TIS001GF`

Always join on `canonical_sku`, never on the raw `product_code` or `sku` column directly.

## Known Data Quality Issues to Model Around

| Issue | Location | Modeling Approach |
|-------|----------|-------------------|
| Mixed date formats | `order_date`, `requested_ship_date` | Normalize in staging via macro; null-check after |
| Null `order_ref` | Fulfillment F-2005, F-2015, F-2099 | Flag as `match_type = 'inferred'` or `'unmatched_fulfillment'` |
| Duplicate fulfillment_id | F-2013 appears twice | Deduplicate in staging using `ROW_NUMBER()` on `(fulfillment_id, ship_date)`, keep latest; flag as `_is_duplicate` |
| Unknown SKU TIS007XX | F-2099 | Will produce `match_type = 'unmatched_fulfillment'`; surface in `mart_unmatched_fulfillments` |
| Partial shipments | F-2003, F-2013, F-2017 notes field | Parse `notes` for `partial` keyword; add `is_partial_shipment` boolean in staging |
| Null `region` | Orders 1005, 1009, 1010, 1015, 1018 | Coalesce to `'UNKNOWN'` in staging; never drop the row |
| Customer name inconsistencies | Orders table | Do not attempt name-matching; `customer_code` is the reliable key |

## Fill Rate Definition

Fill rate is the core business metric:

```
fill_rate_pct = SUM(shipped_qty) / NULLIF(SUM(ordered_qty), 0)
```

- Compute at the `int_orders_fulfillment_joined` grain (one row per order-fulfillment pair).
- Aggregate to product + region grain in `mart_fill_rate`.
- Unmatched orders contribute `shipped_qty = 0` to the denominator.
- Partial shipments (shipped_qty < ordered_qty) are included at face value — do not exclude them.
- Overfilled records (shipped_qty > ordered_qty, possible for inferred matches) are capped at 1.0 fill rate in the mart aggregate using `LEAST(fill_rate_pct, 1.0)`.

## Surrogate Keys

Use `dbt_utils.generate_surrogate_key()` to produce stable surrogate keys for mart tables:
- `mart_fill_rate`: surrogate on `[product_code, region]`
- `mart_unfulfilled_orders`: surrogate on `[order_id]`
- `mart_unmatched_fulfillments`: surrogate on `[fulfillment_id]`

## Grain Rules

- Staging models: same grain as the source (one row per source row, deduplication aside).
- `int_order_match_map`: one row per (order_id, fulfillment_id) pair, plus one row per unmatched order and one per unmatched fulfillment.
- `int_orders_fulfillment_joined`: one row per matched or unmatched entity (same grain as match map).
- `mart_fill_rate`: one row per (product_code, region) combination.
- `mart_unfulfilled_orders`: one row per unfulfilled order.
- `mart_unmatched_fulfillments`: one row per unmatched fulfillment record.

## Business Questions Mapping

| Business Question | Mart Table | Key Columns |
|-------------------|------------|-------------|
| Fill rate by product and region | `mart_fill_rate` | `product_code`, `region`, `fill_rate_pct` |
| Unfulfilled orders and qty at risk | `mart_unfulfilled_orders` | `order_id`, `ordered_qty`, `qty_at_risk` |
| Unmatched fulfillments and volume | `mart_unmatched_fulfillments` | `fulfillment_id`, `shipped_qty`, `volume_at_risk` |
