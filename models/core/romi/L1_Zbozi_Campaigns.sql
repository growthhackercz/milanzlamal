{{ config (
  enabled = true 
) }}
SELECT
  '{{ var('ROMI_MEDIA_BUDGET') }}' AS type,
  '{{ var('ROMI_ZBOZI') }}' AS ad_system,
  'spanario' AS account,
  CAST(premiseId AS STRING) AS account_id,
  LOWER(categoryName) AS categoryZbozi,
  categoryId as campaign_id,
  CAST(impressions AS INT64) AS impressions,
  CAST(clicks AS INT64) AS clicks,
  CAST(spend AS FLOAT64) AS cost,
  PARSE_DATE("%Y-%m-%d", date) AS date

FROM
 {{source('spanario','L0_Zbozi_Categories')}}
