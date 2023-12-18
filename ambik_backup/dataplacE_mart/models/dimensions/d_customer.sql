{{
  config(
    materialized='incremental',
    unique_key = '"CUSTOMER_SK"',
    tags = ["dimensions"]
  )
}}

with
dimension as (
    SELECT  c.doc_nbr as "DOC_NBR",
            COALESCE (c.full_name, 'N/A') AS "CUSTOMER_NAME",
            COALESCE (c.dob, '1901-01-01') AS "DOB",
            created_at AS "CREATED_AT",
            updated_at AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM {{source('marketplace', 'customers')}} c
    {% if is_incremental() %}
    WHERE c.updated_at > (select max("UPDATED_AT") from {{this}})
    {% else %}
    union all
    SELECT  0 as "DOC_NBR",
            'N/A' AS "CUSTOMER_NAME",
            '1901-01-01' AS "DOB",
            '1901-01-01 00:00:00' AS "CREATED_AT",
            '1901-01-01 00:00:00' AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    {% endif %}
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"DOC_NBR"']
    )}} AS "CUSTOMER_SK", *
FROM dimension
