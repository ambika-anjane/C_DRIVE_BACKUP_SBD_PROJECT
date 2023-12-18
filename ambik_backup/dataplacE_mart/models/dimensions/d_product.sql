{{
  config(
    materialized='incremental',
    unique_key = '"PRODUCT_SK"',
    tags = ["dimensions"]
  )
}}

with
dimension as (
    SELECT  p.id as "PRODUCT_ID",
            COALESCE (p.pname, 'N/A') AS "PRODUCT_NAME",
            p.supplier_id AS "SUPPLIER_ID",
            COALESCE(p.price, 0.0) AS "PRODUCT_PRICE",
            created_at AS "CREATED_AT",
            updated_at AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM {{source('marketplace', 'products')}} p
    {% if is_incremental() %}
    WHERE p.updated_at > (select max("UPDATED_AT") from {{this}})
    {% else %}
    union all
    SELECT  0 as "PRODUCT_ID",
            'N/A' AS "PRODUCT_NAME",
            0 AS "SUPPLIER_ID",
            0 AS "PRODUCT_PRICE",
            '1901-01-01 00:00:00' AS "CREATED_AT",
            '1901-01-01 00:00:00' AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    {% endif %}
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"PRODUCT_ID"']
    )}} AS "PRODUCT_SK",
    d."PRODUCT_ID",
    d."PRODUCT_NAME",
    s."SUPPLIER_SK",
    d."PRODUCT_PRICE",
    d."CREATED_AT",
    d."UPDATED_AT",
    d.DW_CREATED_TS,
    d.DW_UPDATED_TS
FROM dimension d inner join {{ref('d_supplier')}} s on s."SUPPLIER_ID" = d."SUPPLIER_ID"
