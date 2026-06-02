-- Fails if any unfulfilled order has a non-positive quantity at risk.
-- qty_at_risk = ordered_qty, which must be positive by source validation.
select qty_at_risk
from {{ ref('mart_unfulfilled_orders') }}
where qty_at_risk <= 0
