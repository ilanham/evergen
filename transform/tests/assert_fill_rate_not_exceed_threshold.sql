{{ config(severity='warn') }}

-- Warns if any product/region has fill_rate_pct > 1.0 before capping.
-- The mart caps at 1.0, so this fires only if the cap logic regresses.
select fill_rate_pct
from {{ ref('mart_fill_rate') }}
where fill_rate_pct > 1.0
