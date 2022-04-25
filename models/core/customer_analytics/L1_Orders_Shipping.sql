{{config(materialized='view')}}

SELECT 
web.code as order_id,
web.itemCode as itemCode,
CASE 
WHEN web.itemCode LIKE 'SHIPPING%' THEN SUM(CAST(REPLACE (web.itemUnitPriceWithoutVat,",",".") AS FLOAT64))
ELSE null
END as order_shipping_cost,
CASE 
WHEN web.itemCode LIKE 'SHIPPING%' THEN web.itemName 
ELSE null
END as order_shipping_name,
CASE 
WHEN web.itemCode LIKE 'BILLING%' THEN SUM(CAST(REPLACE (web.itemUnitPriceWithoutVat,",",".") AS FLOAT64))
ELSE null
END as order_billing_cost,
CASE 
WHEN web.itemCode LIKE 'BILLING%' THEN web.itemName 
ELSE null
END as order_billing_name

FROM {{source('spanario', 'L0_Shoptet_Orders')}} web
WHERE web.itemCode LIKE 'BILLING%' OR web.itemCode LIKE 'SHIPPING%'
GROUP BY web.code, web.itemCode, web.itemUnitPriceWithoutVat, web.itemName
order by web.code desc

  