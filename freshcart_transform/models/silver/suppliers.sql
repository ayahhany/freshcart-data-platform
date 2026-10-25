{{
    config(
        materialized='table',
        schema='silver_suppliers'
    )
}}

with raw_fresh as (
    select 
        supplier,
        productid::text as product_id,
        storeid::text as store_id,
        stockdate::date as stock_date,
        coalesce(quantityonhand, 0)::numeric as quantity_on_hand,
        reorderlevel::numeric as reorder_level,
        _file_name,
        _loaded_at
    from {{ source('bronze_suppliers', 'fresh_stock') }}
),

raw_general as (
    select 
        supplier,
        product::text as product_id,
        store::text as store_id,
        to_date(date, 'DD/MM/YYYY') as stock_date,
        coalesce(qty, 0)::numeric as quantity_on_hand,
        minqty::numeric as reorder_level,
        _file_name,
        _loaded_at
    from {{ source('bronze_suppliers', 'general_stock') }}
),

combined_suppliers as (
    select *, row_number() over (partition by supplier, product_id, store_id, stock_date order by _loaded_at desc) as rn from (
        select * from raw_fresh
        union all
        select * from raw_general
    ) x
)

select 
    supplier,
    product_id,
    store_id,
    stock_date,
    quantity_on_hand,
    reorder_level,
    
    case 
        when quantity_on_hand < 0 then true 
        else false 
    end as is_negative_quantity,
    _file_name,
    _loaded_at
from combined_suppliers
where rn = 1