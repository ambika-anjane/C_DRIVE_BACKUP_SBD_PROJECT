{{
  config(
    materialized='incremental',
    unique_key = '"COMPANY_SK"',
    tags = ["dimensions"]
  )
}}

with
dimension as (
    SELECT  c.cuit as "CUIT",
            COALESCE (c.cname, 'N/A') AS "COMPANY_NAME",
            created_at AS "CREATED_AT",
            updated_at AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM {{source('marketplace', 'companies')}} c
    {% if is_incremental() %}
    WHERE c.updated_at > (select max("UPDATED_AT") from {{this}})
    {% else %}
    union all
    SELECT  0 as "CUIT",
            'N/A' AS "COMPANY_NAME",
            '1901-01-01 00:00:00' AS "CREATED_AT",
            '1901-01-01 00:00:00' AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    {% endif %}
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"CUIT"']
    )}} AS "COMPANY_SK", *
FROM dimension    
