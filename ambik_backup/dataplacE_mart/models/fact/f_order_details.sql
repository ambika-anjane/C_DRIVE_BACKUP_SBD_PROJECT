{# -- Use Surrogate Key 336d5ebc5436534e61d16e63ddfca327 for '-' #}
{{
  config(
    materialized='incremental',
    unique_key = '"ORDER_SK"',
    tags = ["fact"]
  )
}}
with CUST as (
		select "CUSTOMER_SK",
		       "COUNTRY_SK",
		       "USER_AGENT_SK",
		       rank() over (partition by "CUSTOMER_SK" order by "UPDATED_AT" DESC) R
		from {{ref('f_web_log')}}
	), LATEST_CUST AS (
    select * from CUST 
    where R = 1),
 fact as (
    SELECT  
      ordr.id as "ORDER_ID",
      comp."COMPANY_SK" as "COMPANY_SK",
      prd."PRODUCT_SK" as "PRODUCT_SK",
      prd."SUPPLIER_SK" as "SUPPLIER_SK",
      cust."CUSTOMER_SK" AS "CUSTOMER_SK",
      cntry."COUNTRY_SK" as "COUNTRY_SK",
      ua."USER_AGENT_SK" AS "USER_AGENT_SK",
      ordr.order_date as "ORDER_DATE",
      ordr.sale_price as "SALES",
      ordr.created_at AS "CREATED_AT",
      ordr.updated_at AS "UPDATED_AT",
      {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
      {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM  {{source('marketplace', 'orders')}} ordr
          left outer join {{ref('d_company')}} comp on ordr.company_id = COALESCE(comp."CUIT", 0)
          left outer join {{ref('d_product')}} prd ON ordr.product_id = COALESCE(prd."PRODUCT_ID", 0)
          left outer join {{ref('d_customer')}} cust on ordr.customer_id = COALESCE(cust."DOC_NBR", 0)
          left outer join LATEST_CUST lc on lc."CUSTOMER_SK" = cust."CUSTOMER_SK"
          left outer join {{ref('d_country')}} cntry on cntry."COUNTRY_SK" = COALESCE(lc."COUNTRY_SK", '336d5ebc5436534e61d16e63ddfca327') 
          left outer join {{ref('d_user_agent')}} ua on ua."USER_AGENT_SK" = COALESCE(lc."USER_AGENT_SK", '336d5ebc5436534e61d16e63ddfca327')
        {% if is_incremental() %}
    WHERE ordr.updated_at > (select max("UPDATED_AT") from {{this}})
        {% endif %} 
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"ORDER_ID"']
    )}} AS "ORDER_SK", *
FROM fact    
