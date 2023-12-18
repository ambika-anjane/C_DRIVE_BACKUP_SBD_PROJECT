{{ config(materialized = 'incremental',
unique_key = 'prod_code',
merge_update_columns = 'prod_code',


post_hook = "DELETE FROM  increment_append where prod_code not in 
(select prod_code from products)"

)}}

with first_cte as (

    select * from {{ ref('stg_products') }}

    {% if is_incremental() %}
        where prod_code  in  (select prod_code from {{ this }})
    {% endif %}

),

second_cte as (

    select * from {{ ref('stg_products') }}

    {% if is_incremental() %}
        where prod_code  not  in  (select prod_code from {{ this }})
    {% endif %}

),

final as (
    select
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        'U' as "cdc_flag"
        from  
        first_cte
        union  
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        'I' AS "cdc_flag"
        from  
        second_cte

)
select * from final