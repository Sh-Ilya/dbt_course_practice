{{
        config(
            materialized = 'incremental',
            incremental_strategy = 'merge',
            unique_key = ['book_ref'],
            tags = ['bookings'],
            merge_update_columns = ['total_amount']                   
        )
}}

select
  book_ref,
  book_date,
  total_amount
from {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
where 
--  book_date > current_date - interval '7 day'            -- данные более недели назат не могугут поменяться по бизнес логике
  book_date > (select max(book_date) from {{ source('demo_src', 'bookings') }}) - interval '97 day'   -- бронировать можно вперёд на 90 дней и за 7 дней данные могут поменяться
{% endif %}


-- unique_key - обязательный парамер при стратегии merge (указать уник ключ, первичный ключ), если не указать будет append
    