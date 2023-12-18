{{config(
    materialized= 'incremental',
    tags = ['dimension'],
    pre_hook = {"sql" : "CREATE SEQUENCE IF NOT EXISTS channel_code AS INT
                        START 1
                        INCREMENT 1;
                 "}
)}}


WITH source AS (
    SELECT 
            "channel_code",
            "channel_desc",
            "channel_class",
            "insert_dt",
            "last_update_dt"
        FROM {{ref('stg_channel')}}
    ),

    source_qualifier AS (
        SELECT
            "channel_code",
            "channel_desc",
            "channel_class",
            "insert_dt",
            "last_update_dt"
        FROM source
    ),

    lookup_procedure AS (
        SELECT 
            0 as "channel_key",
            '-' as "channel_code"
            {% if is_incremental() %}
            union
            SELECT
            d."channel_key",
            d."channel_code"
        FROM source_qualifier s
        LEFT OUTER JOIN {{this}} d ON d."channel_code" = s."channel_code"
        WHERE d."channel_code" = s."channel_code"
        {% endif %}
    ),

    router_insert AS (
        SELECT 
            l."channel_key" as "channel_key3",
            s."channel_code" as "channel_code3",
            s."channel_desc" as "channel_desc3",
            s."channel_class" as "channel_class3",
            s."insert_dt" as "insert_dt3",
            s."last_update_dt" as "last_update_dt3"
        FROM source_qualifier s
        LEFT JOIN lookup_procedure l ON s."channel_code" = l."channel_code"
        WHERE l."channel_key" IS NULL
    ),

        router_update AS (
        SELECT 
            l."channel_key" as "channel_key1",
            s."channel_code" as "channel_code1",
            s."channel_desc" as "channel_desc1",
            s."channel_class" as "channel_class1",
            s."insert_dt" as "insert_dt1",
            s."last_update_dt" as "last_update_dt1"
        FROM source_qualifier s
        LEFT JOIN lookup_procedure l ON s."channel_code" = l."channel_code"
        WHERE l."channel_key" IS NOT NULL
    ),

        sequence_col AS (
        SELECT nextval('channel_code') AS "nextval",
            currval('channel_code') AS "currval"
    ),

    --     expression AS(
    --         SELECT
    --             current_timestamp AS "systimestamp",
    --             "in_channel_code"
    --         from lookup_procedure
    -- ),

        update_stratagy_ins AS (
            SELECT 
                nextval('channel_code') AS "channel_key",
                "channel_code3" AS "channel_code",
                "channel_desc3" AS "channel_desc",
                "channel_class3" AS "channel_class",
                "insert_dt3" AS "insert_dt",
                "last_update_dt3" AS "last_update_dt"
            FROM router_insert
    ),

        update_stratagy_upd AS (
            SELECT 
                nextval('channel_code') AS "channel_key",
                "channel_code1" AS "channel_code",
                "channel_desc1" AS "channel_desc",
                "channel_class1" AS "channel_class",
                "insert_dt1" AS "insert_dt",
                "last_update_dt1" AS "last_update_dt"
            FROM router_update
    ),
        dim_channel AS (
            SELECT *,
                current_timestamp AS "dw_insert_dt",
                current_timestamp AS "dw_update_dt",
                concat("channel_key","channel_code") AS "md5_checksum",
                CASE WHEN "channel_key" IS NULL THEN 'insert'
                    END AS "cdc_flag",
                current_timestamp AS "channel_eff_from",
                current_timestamp AS "channel_eff_to"
            FROM update_stratagy_ins
            UNION ALL
            SELECT *,
                current_timestamp AS "dw_insert_dt",
                current_timestamp AS "dw_update_dt",
                concat("channel_key","channel_code") AS "md5_checksum",
                CASE WHEN "channel_key" IS NOT NULL THEN 'update'
                    END AS "cdc_flag",
                current_timestamp AS "channel_eff_from",
                current_timestamp AS "channel_eff_to"                    
            FROM update_stratagy_upd
    )
        
    select * from dim_channel