{{ config(materialized='table') }}

SELECT *
FROM {{source('spanario', 'L0_Shoptet_Stocks')}} sto