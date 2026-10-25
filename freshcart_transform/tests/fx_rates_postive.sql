select rate_date, currency, rate_to_usd
from {{ ref('fx_rates') }}
where rate_to_usd <= 0