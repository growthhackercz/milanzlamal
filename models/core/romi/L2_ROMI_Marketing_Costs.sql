-- Assumption - there is always L1_ROMI_Additional_Costs
SELECT
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  clicks,
  impressions,
  cost,
  date
FROM {{ref('L1_ROMI_Additional_Costs')}}
{% if var('ROMI_CONFIG').google %}
UNION ALL
SELECT
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  clicks,
  impressions,
  cost,
  date
FROM {{ref('L1_GA_Google_Ads')}}
{% endif %}
{% if var('ROMI_CONFIG').sklik %}
UNION ALL
SELECT
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  clicks,
  impressions,
  cost,
  date
FROM {{ref('L1_Sklik_Campaigns')}}
{% endif %}
{% if var('ROMI_CONFIG').facebook %}
UNION ALL
SELECT
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  clicks,
  impressions,
  cost,
  date
FROM {{ref('L1_FB_Ads_Campaigns')}}
{% endif %}
{% if var('ROMI_CONFIG').heureka %}
UNION ALL
SELECT 
  type,
  ad_system,
  account,
  account_id,
  productName AS campaign,
  categoryHeureka AS campaign_id,
  visits AS clicks,
  0 as impressions,
  cost,
  date
FROM {{ref('L1_Heureka_Costs')}}
{% endif %}

{% if var('ROMI_CONFIG').zbozi %}
UNION ALL
SELECT 
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  clicks,
  impressions,
  cost,
  date
FROM {{ref('L1_Zbozi_Campaigns')}}
{% endif %}