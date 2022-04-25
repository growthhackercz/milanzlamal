SELECT
  '{{ var('ROMI_MEDIA_BUDGET') }}' AS type,
  '{{ var('ROMI_FACEBOOK') }}' AS ad_system,
  LOWER(campaigns.account_name) AS account,
  LOWER(campaigns.account_id) AS account_id,
  {{string_unify('campaigns.campaign_name')}} AS campaign,
  LOWER(campaigns.campaign_id) AS campaign_id,
  SUM(CAST(campaigns.clicks AS INT64)) AS clicks,
  SUM(CAST(campaigns.impressions AS INT64)) AS impressions,
  SUM(CAST(campaigns.spend AS FLOAT64)) AS cost,
  PARSE_DATE('%Y-%m-%d', campaigns.date_start) AS date
FROM
  {{source('spanario','L0_FB_Ads_Campaigns_insights')}} AS campaigns
GROUP BY
  type,
  ad_system,
  account,
  campaigns.account_id,
  campaign,
  campaigns.campaign_id,
  date
