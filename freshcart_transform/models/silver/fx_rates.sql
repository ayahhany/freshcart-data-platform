{{
    config(
        materialized='view',
        schema='silver_fx'
    )
}}

with raw_fx as (
    select 
        date,
        currency,
        rate_to_usd,
        _loaded_at,
       
        row_number() over (partition by date, currency order by _loaded_at desc) as rn
    from {{ source('bronze_fx', 'rates') }}
)

select 
    cast(date as date) as rate_date,
    upper(trim(currency)) as currency,
    cast(rate_to_usd as numeric(18, 6)) as rate_to_usd,
    _loaded_at
from raw_fx
where rn = 1