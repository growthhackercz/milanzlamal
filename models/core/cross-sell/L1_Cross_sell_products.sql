{{ config(materialized = 'view') }}
SELECT
  DISTINCT 
  productName product_name,
  transactionId document_id,
  DATE(date) document_date
FROM
 {{source('spanario','L0_GA_Cross_sell')}}
WHERE
  productName IS NOT NULL
  AND transactionId IS NOT NULL
  AND transactionId <> ""
  AND productName <> ""
  -- AND productName NOT IN (
    -- ".",
    -- "Ekologická likvidace",
    -- "Dopravné a balné",
    -- "Přihlášení, převod vozidla na dopravním inspektorátu",
    -- "Režijní práce",
    -- "DIAGNOSTIKA",
    -- "Odvoz motocyklu",
    -- "Náhradní vozidlo",
    -- "SM - spotřební materiál"
  -- )
  -- AND productName NOT LIKE "%Servisní práce%"
  -- AND productName NOT LIKE "NEPOUZIVAT%"
  -- AND productName NOT LIKE "%demontáž, přezutí a vyvážení"