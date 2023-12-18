{{ config(
   materialized = 'incremental',
   tag = ['dimension'],
   unique_key = 'customer_id'
)
}}

with source as(
select  distinct
    "customer_id",
    "first_name",
    "last_name",
    "address",
    "city",
    "state",
    "zip_code",
    "marital_status"
from  {{ref('stg_customer_extract')}}
),


{% if is_incremental() %}
RTR_Decide_Update_new_records as(

  select
     s."customer_id",
     s."first_name",
     s."last_name",
     s."address",
     s."city",
     s."state",
     s."zip_code",
     s."marital_status"
    
     from 
     source s  
     where s."customer_id" is not null
     
)
{% endif %},


RTR_Decide_insert_new_records as(

  select
    
     s."first_name",
     s."last_name",
     s."address",
     s."city",
     s."state",
     s."zip_code",
     s."marital_status"
     from 
     source s
     where 
     {% if is_incremental() %}
     s."customer_id" is  null
     {% endif %}
    
     
),
dim_upgrade as(
       
        select
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        'I' as "cdc_flag"
        from  
        RTR_Decide_Update_new_records
     
)
select * from dim_upgrade





