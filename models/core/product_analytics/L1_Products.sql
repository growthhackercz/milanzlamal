{{ config(materialized='table') }}

SELECT 
CASE when purchasePrice = '' then CAST(0 AS FLOAT64) else ROUND((CAST(REPLACE (price,",",".") AS FLOAT64))-(CAST(REPLACE (purchasePrice,",",".") AS FLOAT64)),2) END AS item_margin,
manufacturer as item_manufacturer,
name as item_name,
defaultCategory as item_category

FROM {{source('spanario', 'L0_Shoptet_Products')}} pro