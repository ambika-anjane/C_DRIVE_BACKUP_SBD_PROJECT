{{
    config(
        materialized='table'
    )
}}

select distinct PROMO_CODE,PROMO_NAME,PROMO_SUBCATEGORY,PROMO_CATEGORY,PROMO_COST,
PROMO_BEGIN_DATE,PROMO_END_DATE,PROMO_TOTAL,INSERT_DT,LAST_UPDATE_DT 
From {{source('public','promotions')}} 

