{{
  config(
    materialized='incremental',
    unique_key = '"LOG_SK"',
    tags = ["fact"]
  )
}}

with fact as (
    SELECT  
      l.id as "LOG_ID",
      l.host_ip as "HOST_IP",
      c2."COUNTRY_SK" as "COUNTRY_SK",
      c."CUSTOMER_SK" AS "CUSTOMER_SK",
      l.request_type as "REQUEST_TYPE",
      l.referer as "REFERER",
      u."USER_AGENT_SK" as "USER_AGENT_SK",
      l.log_time AS "CREATED_AT",
      l.log_time AS "UPDATED_AT",
      {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
      {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM  {{source('weblog', 'logs')}} l,
          {{ref('d_customer')}} c,
          {{ref('d_country')}} c2,
          {{ref('d_user_agent')}} u
    WHERE
        {% if is_incremental() %}
          l.log_time > (select max("UPDATED_AT") from {{this}}) AND
        {% endif %} 
          replace(lower(c."CUSTOMER_NAME"), ' ', '_') = l.username
      and c2."COUNTRY_NAME" = l.COUNTRY
      and u."USER_AGENT" = l.USER_AGENT
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"LOG_ID"']
    )}} AS "LOG_SK", *
FROM fact    
