{{
        config(
            materialized = 'incremental',
            incremental_stratege = 'append',
            tags = ['bookings']       
        )
}}

select
  book_ref,
  book_date,
  total_amount
from {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
where 
  ('0x' || book_ref)::bigint > (select max(('0x' || book_ref)::bigint) from {{this}})
{% endif %}

    