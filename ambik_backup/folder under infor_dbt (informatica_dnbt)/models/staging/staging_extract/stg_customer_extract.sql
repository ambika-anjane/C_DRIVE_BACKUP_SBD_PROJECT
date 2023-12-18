{{
    config(
        materialized='table',
        tags = ['staging']
    )
}}

select  * from {{source('public','customer_extract')}}