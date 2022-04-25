{{ config ( materialized= 'view',enabled=true )}} 
SELECT

  '{{ var('ROMI_MEDIA_BUDGET') }}' AS type,
  '{{ var('ROMI_HEUREKA') }}' AS ad_system,
  LOWER(shopName) AS account,
  CAST(shopId AS STRING) AS account_id,
  {{string_unify("categoryHeureka")}} AS categoryHeureka,
  LOWER(categoryShop) AS categoryShop,
  LOWER(productNameHeureka) AS productName,
  CAST(itemIdShop AS STRING) AS itemIdShop,
  CAST(visitsTotal AS INT64) AS visits,
  CAST(expensesTotal AS FLOAT64) AS cost,
  PARSE_DATE("%Y-%m-%d", date) AS date

FROM
  {{ref('LI_Heureka_Costs')}}