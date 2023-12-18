{{
  config(
    materialized='incremental',
    unique_key = '"COUNTRY_SK"',
    tags = ["dimensions"]
  )
}}

with dimension as (
    SELECT distinct l.country as "COUNTRY_NAME",
           {{ dbt_utils.current_timestamp() }} AS "CREATED_AT",
           {{ dbt_utils.current_timestamp() }} AS "UPDATED_AT",
           {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
           {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM  {{source('weblog', 'logs')}} l
    {% if is_incremental() %}
    WHERE l.log_time > (select max("UPDATED_AT") from {{this}})
    {% else %}
    union
    SELECT  '-' as "COUNTRY_CODE",
            '1901-01-01 00:00:00' AS "CREATED_AT",
            '1901-01-01 00:00:00' AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    {% endif %}
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"COUNTRY_NAME"']
    )}} AS "COUNTRY_SK", *
FROM dimension    
