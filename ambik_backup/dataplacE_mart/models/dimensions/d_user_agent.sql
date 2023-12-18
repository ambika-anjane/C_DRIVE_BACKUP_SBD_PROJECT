{{
  config(
    materialized='incremental',
    unique_key = '"USER_AGENT_SK"',
    tags = ["dimensions"]
  )
}}

with dimension as (
    SELECT  l.user_agent as "USER_AGENT",
            max(l.log_time) AS "CREATED_AT",
            max(l.log_time) AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    FROM  {{source('weblog', 'logs')}} l
    {% if is_incremental() %}
    WHERE l.log_time > (select max("UPDATED_AT") from {{this}})
    group by l.user_agent
    {% else %}
    group by l.user_agent
    union all
    SELECT  '-' as "USER_AGENT",
            '1901-01-01 00:00:00' AS "CREATED_AT",
            '1901-01-01 00:00:00' AS "UPDATED_AT",
            {{ dbt_utils.current_timestamp() }} as DW_CREATED_TS,
            {{ dbt_utils.current_timestamp() }} as DW_UPDATED_TS
    {% endif %}
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"USER_AGENT"']
    )}} AS "USER_AGENT_SK", *
FROM dimension    
