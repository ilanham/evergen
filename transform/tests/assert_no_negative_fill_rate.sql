-- Fails if any product/region combination has a negative fill rate.
-- A negative fill rate is physically impossible and indicates a data error.
select fill_rate_pct
from {{ ref('mart_fill_rate') }}
where fill_rate_pct < 0
