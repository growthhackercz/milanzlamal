{{ config(materialized='view') }}
-- v závěru kódu je vstupní soubor povinných hodnot
--neco noveho
SELECT 
  -- věci, které je potřeba vyplnit manuálně 
  true as order_online,
  LOWER(web.statusName) LIKE 'storno%'AS order_cancelled, --vrací hodnotu true když splňuje a zde místo STORNO% musíš zadat jak to mají oni pojmenované v případě stornované objednávky

  -- tento case tu je jen když řeším storna
  -- jinak je tu jen toto AVG(round((CAST (REPLACE (web.totalPriceWithoutVat,",",".") AS FLOAT64)*(1 / CAST (exchangeRate AS FLOAT64))),2))  as order_revenue,
  CASE
    WHEN AVG(CAST(REPLACE(web.totalPriceWithoutVat,",",".") AS FLOAT64)) > 0 THEN AVG(round((CAST (REPLACE (CAST(web.totalPriceWithoutVat AS STRING),",",".") AS FLOAT64)*(1 / CAST (web.exchangeRate AS FLOAT64))),2))
    ELSE AVG(ROUND((CAST(REPLACE(CAST(storno.order_revenue AS STRING),",",".") AS FLOAT64)*(1 / CAST (web.exchangeRate AS FLOAT64))),2))
  END AS order_revenue,

  AVG(order_margin) as order_margin,
  /*
  CASE
    WHEN CAST(order_margin AS FLOAT64) is not null THEN AVG(round(CAST (order_margin AS FLOAT64),2))
    ELSE CAST(null as FLOAT64)
  END AS order_margin,*/


--  IF(CAST( AS FLOAT64) > 0,0,SUM(ROUND(CAST(order_margin AS FLOAT64),2))) as order_margin,

  --informace o objednávce,
  CAST(web.date AS TIMESTAMP) AS order_date,
  web.code AS order_id,
  web.statusName AS order_status,
  web.paid AS order_paid,
 
  -- doplňkové informace o objednávce
  AVG(round((CAST (REPLACE (web.totalPriceVat, ",", "." ) AS FLOAT64 )*(1 / CAST (web.exchangeRate AS FLOAT64))),2)) as order_tax,
  web.currency AS currency,
  CAST (web.exchangeRate AS FLOAT64) as exchange_rate,
  -- nedává smysl v reportech pak - je to vypočítané v DS - ROUND(CASE WHEN SUM(CAST(REPLACE(web.totalPriceWithoutVat,",",".") AS FLOAT64)) > 0 THEN IF(AVG(mar.order_margin) > 0, (AVG(mar.order_margin) / AVG(round((CAST (REPLACE (web.totalPriceWithoutVat,",",".") AS FLOAT64)*(1 / CAST (exchangeRate AS FLOAT64))),2))),null) ELSE NULL END,4) as order_margin_rate,
 


  --AVG(ROUND(CAST(order_margin AS FLOAT64),2)) as order_margin,
   /* CASE 
      WHEN CAST(order_margin_rate AS STRING) <> '' THEN CAST(REPLACE (CAST(web.totalPriceWithoutVat AS STRING),",",".") AS FLOAT64) * IF(CAST(order_margin_rate AS FLOAT64) = 0,1,CAST(order_margin_rate AS FLOAT64))
      ELSE CAST(null AS FLOAT64)
    END) AS order_margin,*/

  --tohle není pravda pokud všechny položky nemají marži. Řešit to tedy až v L2 a tady poouze pokud to nechci řešit na úrovni produktů.
 /*
  IF(AVG(CASE 
    WHEN CAST(order_margin_rate AS STRING) <> '' THEN  CAST(REPLACE (CAST(web.totalPriceWithoutVat AS STRING),",",".") AS FLOAT64) * CAST(order_margin_rate AS FLOAT64)
    ELSE CAST(null AS FLOAT64)
  END) > 0, true, false) AS known_margin,*/


  -- informace o zákazníkovi
  web.code AS document_id,
  web.email AS customer_id,
  web.email AS customer_email,
  web.billFullName AS customer_name,
  web.phone AS customer_phone,
  web.billCity AS customer_city,
  web.billZip AS customer_zip,
  'maloobchod' as customer_type,
  web.billCountryName AS customer_country,
  
  --informace o zdroji objednávky
   ga.source acquisition_source,
   ga.medium acquisition_medium,
   ga.campaign acquisition_campaign,

FROM {{source('spanario', 'L0_Shoptet_Orders')}} web
LEFT JOIN {{source('spanario', 'L0_GA_Transactions')}} ga ON ga.transactionId = CAST(web.code AS STRING)
LEFT JOIN {{ref('L1_Orders_Margin')}} mar ON mar.order_id = web.code

-- SHOPTET -- hodnota stornovaných objednávek
LEFT JOIN (SELECT order_id, order_revenue FROM {{ref('L1_Orders_Margin')}}) storno ON CAST(storno.order_id AS STRING) = CAST(web.code AS STRING)

WHERE itemCode NOT LIKE 'SHIPPING%' AND itemCode NOT LIKE 'BILLING%' AND itemCode NOT LIKE 'COUPON%'
Group By web.code, web.email, web.currency, web.exchangeRate, web.date, web.statusName, web.paid,web.email, web.phone, web.billFullName,web.billCity,web.billCountryName, web.billZip, ga.source, ga.medium, ga.campaign
Order by web.date desc 

--Předpoklad pro základní fungování reportu
  -- customer_id:STRING, 
  -- customer_email:STRING,
  -- customer_phone:STRING,
  -- customer_name:STRING,
  -- acquisition_source:STRING,
  -- acquisition_medium:STRING,
  -- acquisition_campaign:STRING,
  -- order_id:STRING,
  -- order_date:TIMESTAMP,
  -- order_status:STRING,
  -- order_revenue:FLOAT,
  -- order_cancelled:BOOLEAN,
  -- order_margin:FLOAT, 
  -- order_online:BOOLEAN,
  -- known_margin:BOOLEAN