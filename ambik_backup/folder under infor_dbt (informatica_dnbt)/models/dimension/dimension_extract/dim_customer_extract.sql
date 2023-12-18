{{ config(
   materialized = 'incremental',
   tag = ['dimension'],
   unique_key = 'customer_id',
   merge_update_columns = 'customer_id',
  

   

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

sq_customer_extract as(
select  distinct
    "customer_id",
    "first_name",
    "last_name",
    "address",
    "city",
    "state",
    "zip_code",
    "marital_status"
    from source
),


lkp_customers as(
     select s."customer_id" ,
     s."first_name",
     s."last_name",
     s."address",
     s."city",
     s."state",
     s."zip_code",
     s."marital_status"

     from sq_customer_extract s
     {% if is_incremental() %}
     union 
     select 
     d."customer_id",
     d."first_name",
     d."last_name",
     d."address",
     d."city",
     d."state",
     d."zip_code",
     d."marital_status"
     from {{ this }} d
     left join sq_customer_extract s
     on s."customer_id"  = d."customer_id"
     {% endif %}
),
exp_detectchanges as (
 select
     l."customer_id",
     s."address",
     l."address" as "pm_prev_address",
     s."city",
     l."city" as "pm_prev_city",
     s."first_name",
     l."first_name" as "pm_prev_first_name",
     s."last_name",
     l."last_name" as "pm_prev_last_name",
     s."marital_status",
     l."marital_status" as "pm_prev_marital_status",
     s."state",
     l."state" as "pm_prev_state",
     s."zip_code",
     l."zip_code" as "pm_prev_zip_code",
     current_timestamp as "effective_date"
     from sq_customer_extract s left join 
     lkp_customers l
     on s."customer_id" = l."customer_id"
   
),
 
 rtr_decide_insert_new_records as(
     select
     l."customer_id" as "prev_customer_id",

     s."customer_id",
     COALESCE(l."customer_id",'Empty') as "new_flag",
     s."first_name",
     s."last_name",
     s."address",
     s."city",
     s."state",
     s."zip_code",
     s."marital_status",
     e."effective_date"
     from 
     lkp_customers l left join sq_customer_extract s  
     on  s."customer_id" = l."customer_id"
     left join exp_detectchanges e
     on  e."customer_id" = l."customer_id"
     {% if is_incremental() %}
    where l."customer_id" is   null
     {% endif %}
 ),

 rtr_decide_update_old_records as(
     select
     l."customer_id" as "prev_customer_id",
     s."customer_id",
     COALESCE(l."customer_id",'Not Empty') as "changed_flag",
     s."first_name",
     s."last_name",
     s."address",
     s."city",
     s."state",
     s."zip_code",
     s."marital_status",
     e."effective_date"
     from 
     lkp_customers l left join sq_customer_extract s  
     on  s."customer_id" = l."customer_id"
     left join exp_detectchanges e
     on  e."customer_id" = l."customer_id"
    {% if is_incremental() %}
    where l."customer_id" is not null
    {% endif %}
 
 ),
 

 dim_test as(
       
        
     
        select
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        'I' as "cdc_flag"
        from rtr_decide_insert_new_records

        union

        select
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        'U' as "cdc_flag"
        from rtr_decide_update_old_records
        
        
        
        
)
select * from dim_test






 

