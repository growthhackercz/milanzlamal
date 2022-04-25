{{ config (
  enable=false
) }}

SELECT
  '{{ var('ROMI_MEDIA_BUDGET') }}' AS type,
  '{{ var('ROMI_SKLIK') }}' AS ad_system,
  LOWER(userName) AS account,
  LOWER(accountId) AS account_id,
  {{string_unify("campaigns.name")}} AS campaign,
  LOWER(stats.id) AS campaign_id,
  SUM(CAST(clicks AS INT64)) AS clicks,
  SUM(CAST(impressions AS INT64)) AS impressions,
  SUM(CAST(stats.totalMoney AS FLOAT64)*0.01) AS cost,
  PARSE_DATE("%Y%m%d", stats.date) AS date
FROM
  {{source('spanario','L0_Sklik_Campaigns_Stats')}} as stats
  /*
  Join data about camapign names and status.
  */
  LEFT JOIN (
    SELECT
      campaigns.id,
      campaigns.accountId,
      campaigns.name,
      campaigns.status,
      accounts.userName
    FROM
      {{source('spanario','L0_Sklik_Campaigns')}} AS campaigns
      /*
      Join data about account name.
      */
      LEFT JOIN (
        SELECT
          accounts.userId,
          accounts.userName
        FROM
          {{source('spanario','L0_Sklik_Accounts')}} AS accounts
      ) AS accounts on accounts.userId = campaigns.accountId
  ) AS campaigns on campaigns.id = stats.id
where
  stats.date != ""
GROUP BY
  type,
  ad_system,
  account,
  account_id,
  campaign,
  campaign_id,
  date
