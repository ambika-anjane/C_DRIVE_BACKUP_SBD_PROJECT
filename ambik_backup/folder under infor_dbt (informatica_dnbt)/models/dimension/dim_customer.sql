{{
  config(
    materialized='incremental',
    tag = ['dimensions'],
    pre_hook ={ 
      "sql":  "create sequence if not exists cust_code as int
        increment 1
        start 1;"
    }
  )
}}

with source as (
    select  
        "cust_code",
        "cust_first_name",
        "cust_last_name",
        "cust_gender",
        "cust_year_of_birth",
        "cust_marital_status",
        "cust_street_address",
        "cust_postal_code",
        "cust_city",
        "cust_state_province",
        "country_code",
        "cust_main_phone_number",
        "cust_income_level",
        "cust_credit_limit",
        "cust_email",
        "insert_date",
        "last_update_dt",
        "cust_valid" 
    from {{ref('stg_customer')}}
),

sq_customer as (
    select 
        "cust_code",
        "cust_first_name",
        "cust_last_name",
        "cust_gender",
        "cust_year_of_birth",
        "cust_marital_status",
        "cust_street_address",
        "cust_postal_code",
        "cust_city",
        "cust_state_province",
        "country_code",
        "cust_main_phone_number",
        "cust_income_level",
        "cust_credit_limit",
        "cust_email",
        "insert_date",
        "last_update_dt",
        "cust_valid"
    from source
),



-- incremental as(
--     select * from lkp_dim_cus
--     {%- if is_incremental() -%}
--         where "dw_insert_dt" > (select max("dw_insert_dt") from {{this}})
--     {%- endif -%}
-- )

lkp_dim_cus as(
    select 0 as "cust_key",
           '-' as "cust_code"
    {% if is_incremental() %}
    union
    select  
    d."cust_key",
    d."cust_code"
    from sq_customer s  
    left outer join {{this}} d on d."cust_code" = s."cust_code"
    where d."cust_code" = s."cust_code"
    {% endif %}
),

exp_trans as(
    select distinct
    current_timestamp as "systemdate"
    from lkp_dim_cus
),

router_trans_insert as(
    select 
        d."cust_key",
        c."cust_code",
        c."cust_first_name",
        c."cust_last_name",
        c."cust_gender",
        c."cust_year_of_birth",
        c."cust_marital_status",
        c."cust_street_address",
        c."cust_postal_code",
        c."cust_city",
        c."cust_state_province",
        c."country_code",
        c."cust_main_phone_number",
        c."cust_income_level",
        c."cust_credit_limit",
        c."cust_email",
        c."insert_date",
        c."last_update_dt",
        c."cust_valid"
        from sq_customer c 
      --  inner join exp_trans e on e."in_cust_code" = c."cust_code"
        left join lkp_dim_cus d on d."cust_code" = c."cust_code"
        where d."cust_key" is null
),

router_trans_update as(
    select 
        d."cust_key",
        c."cust_code",
        c."cust_first_name",
        c."cust_last_name",
        c."cust_gender",
        c."cust_year_of_birth",
        c."cust_marital_status",
        c."cust_street_address",
        c."cust_postal_code",
        c."cust_city",
        c."cust_state_province",
        c."country_code",
        c."cust_main_phone_number",
        c."cust_income_level",
        c."cust_credit_limit",
        c."cust_email",
        c."insert_date",
        c."last_update_dt",
        c."cust_valid"
        from sq_customer c 
      --  inner join exp_trans e on e."in_cust_code" = c."cust_code"
        left join lkp_dim_cus d on d."cust_code" = c."cust_code"
        where d."cust_key" is not null
),

upd_insert_dim_customers as(
    select
       -- "systemdate",
        nextval('cust_code') as "cust_key",
        "cust_code",
        "cust_first_name",
        "cust_last_name",
        "cust_gender",
        "cust_year_of_birth",
        "cust_marital_status",
        "cust_street_address",
        "cust_postal_code",
        "cust_city",
        "cust_state_province",
        "country_code",
        "cust_main_phone_number",
        "cust_income_level",
        "cust_credit_limit",
        "cust_email",
        "insert_date",
        "last_update_dt",
        "cust_valid"
        from router_trans_insert 
),

upd_update_dim_customers as(
    select
       -- "systemdate",
        nextval('cust_code') as "cust_key",
        "cust_code",
        "cust_first_name",
        "cust_last_name",
        "cust_gender",
        "cust_year_of_birth",
        "cust_marital_status",
        "cust_street_address",
        "cust_postal_code",
        "cust_city",
        "cust_state_province",
        "country_code",
        "cust_main_phone_number",
        "cust_income_level",
        "cust_credit_limit",
        "cust_email",
        "insert_date",
        "last_update_dt",
        "cust_valid"
        from router_trans_update 
),

dim_customers as(
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        concat("cust_key","cust_code") as "md5_checksum",
        case when "cust_key" is null then 'insert'
        end as "cdc_flag",
        current_timestamp as "cust_eff_from", 
        current_timestamp as "cust_eff_to"
        from  
        upd_insert_dim_customers
        union all
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        concat("cust_key","cust_code") as "md5_checksum",
        case when "cust_key" is not null then 'update'
        end as "cdc_flag",
        current_timestamp as "cust_eff_from", 
        current_timestamp as "cust_eff_to"
        from  
        upd_update_dim_customers
)

select * from dim_customers 

