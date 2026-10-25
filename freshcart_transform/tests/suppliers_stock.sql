select supplier, product_id, store_id, quantity_on_hand
from {{ ref('suppliers') }}
where is_negative_quantity = true