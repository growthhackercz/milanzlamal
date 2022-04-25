{{ config(materialized = 'view') }}

SELECT
  DISTINCT 
  productCategoryHierarchy product_name,
  transactionId document_id,
  DATE(date) document_date
FROM
 {{source('spanario','L0_GA_Cross_sell')}}
WHERE
  productCategoryHierarchy IS NOT NULL
  AND transactionId IS NOT NULL
  AND transactionId <> ""
  AND productCategoryHierarchy <> ""