{{config(materialized='view')}}
-- podmínky jsou aby purchasePrice byla uvedena S DPH, jiank je potřeba oddělat to dělení tím DPH.
-- order revenue je pak také bez DPH.

WITH Margin AS (
  SELECT
    code,
    date,
    orderItemCode,
    orderItemUnitPurchasePrice AS purchasePrice,
  FROM 
  {{source('spanario', 'L0_Shoptet_Order_Margin')}}
)

SELECT 
  web.code as order_id,
  ROUND(AVG(CASE WHEN margin.purchasePrice <> '' THEN (CAST(REPLACE (web.itemTotalPriceWithoutVat,",",".") AS FLOAT64)) - ((CAST(REPLACE (margin.purchasePrice,",",".") AS FLOAT64)) / (CAST((100 + CAST(web.itemVatRate as FLOAT64)) as FLOAT64))*100 * (CAST(itemAmount AS FLOAT64))) * (1 / CAST (web.exchangeRate AS FLOAT64)) 
  else CAST(null as FLOAT64) END),2) AS order_margin,

  ROUND(AVG(CASE WHEN web.itemTotalPriceWithoutVat <> '' THEN CAST(REPLACE (web.totalPriceWithoutVat,",",".") AS FLOAT64) * (1 / CAST (web.exchangeRate AS FLOAT64))
  else CAST(null as FLOAT64) END),2) AS order_revenue,

FROM {{source('spanario', 'L0_Shoptet_Orders')}} web
LEFT JOIN margin ON web.itemCode = margin.orderItemCode

GROUP BY web.code
