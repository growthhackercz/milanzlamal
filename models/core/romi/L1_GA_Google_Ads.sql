SELECT
  '{{ var('ROMI_MEDIA_BUDGET') }}' AS type,
  '{{ var('ROMI_GOOGLE_ADS') }}' AS ad_system,
  LOWER(idProfile) AS account,
  LOWER(idProfile) AS account_id,
  {{string_unify("campaign")}} AS campaign,
  LOWER(adwordsCampaignID) AS campaign_id,
  SUM(CAST(adClicks AS INT64)) AS clicks,
  SUM(CAST(impressions AS INT64)) AS impressions,
  SUM(CAST(adCost AS FLOAT64)) AS cost,
  PARSE_DATE("%Y-%m-%d", date) AS date
FROM
  {{source('spanario','L0_GA_Google_Ads')}}
GROUP BY
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  date
