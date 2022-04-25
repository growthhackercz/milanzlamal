{{ config(materialized='view') }}
WITH order_items AS (
  SELECT 
    ARRAY_AGG (
      STRUCT(
        -- základní informace o produktech
        products.code as item_id,
        products.ean as item_sku,
        products.defaultCategory as item_category,
        products.categoryText as item_category_name,
        products.googleCategoryId as item_google_product_category,
        -- ceny produktů
       CAST(REPLACE(products.price,",",".") AS FLOAT64) / (CAST((100 + CAST(products.percentVat	 as FLOAT64)) as FLOAT64)) * 100 * (1 / CAST (products.priceRatio AS FLOAT64))  as item_revenue,
       IF (products.purchasePrice <>'',CAST(REPLACE(products.purchasePrice,",",".") AS FLOAT64) / (CAST((100 + CAST(products.percentVat as FLOAT64)) as FLOAT64)) * 100 * (1 / CAST (products.priceRatio AS FLOAT64)),null)  as item_purchase_price,
       IF (products.purchasePrice <> '', CAST(REPLACE (products.price,",",".") AS FLOAT64) - CAST(REPLACE (products.purchasePrice,",",".") AS FLOAT64) / (CAST((100 + CAST(products.percentVat as FLOAT64)) as FLOAT64) * 100) * (1 / CAST (products.priceRatio AS FLOAT64)), null) as item_margin,
       IF(IF (products.purchasePrice <> '', CAST(REPLACE (products.price,",",".") AS FLOAT64) - CAST(REPLACE (products.purchasePrice,",",".") AS FLOAT64) / (CAST((100 + CAST(products.percentVat as FLOAT64)) as FLOAT64) * 100) * (1 / CAST (products.priceRatio AS FLOAT64)), null) > 0,true,false) AS item_known_margin,
       -- množství produktů
       stocks.stock as quantity,
       products.manufacturer as manufacturer
      )
    ) AS product_bucket, 
    ordit.code

  FROM 
    {{source('spanario', 'L0_Shoptet_Orders') }} ordit 
    LEFT JOIN {{source('spanario', 'L0_Shoptet_Products') }} products ON ordit.itemCode = products.code
    LEFT JOIN {{source('spanario', 'L0_Shoptet_Stocks') }} stocks ON ordit.itemCode = stocks.itemCode 
  GROUP BY 
    ordit.code
) 
-- Pokračuje normální tabulka
SELECT
  ord.*,
  order_items.* EXCEPT (code),
  ship.* EXCEPT(order_id),
  bill.* EXCEPT (order_id)
/*kupony musím nechat protože někdy sníží hodnotu objednávky procentuálně a propíše se to hned do hodnoty objednávky a jindy hodnota produktu zůstane stejná, ale odečte se konkrétní částka*/
 
FROM {{ref('L1_Orders')}} ord
LEFT JOIN order_items ON order_items.code = ord.order_id
LEFT JOIN (SELECT order_id, order_shipping_cost,order_shipping_name FROM {{ref('L1_Orders_Shipping')}} WHERE order_shipping_name IS NOT NULL) ship ON ship.order_id = ord.order_id
LEFT JOIN (SELECT order_id, order_billing_cost,order_billing_name FROM {{ref('L1_Orders_Shipping')}} WHERE order_billing_name IS NOT NULL) bill ON bill.order_id = ord.order_id

order by order_date desc

