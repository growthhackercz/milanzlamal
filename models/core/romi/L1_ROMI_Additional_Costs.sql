SELECT
  costs.type,
  ad_system,
  costs.account,
  costs.account_id,
  costs.campaign,
  costs.campaign_id,
  costs.clicks,
  costs.impressions,
  calendar.delimiter_date as date,
  (
    costs.cost / CAST(calendar.days_in_month AS NUMERIC)
  ) AS cost
FROM
  {{source('spanario','L0_ROMI_Additional_Costs')}} AS costs
  LEFT JOIN (
    SELECT
      calendar.delimiter_date,
      calendar.yearMonth,
      calendar.days_in_month
    FROM
      {{source('auxiliary_tables_EU','L0_Calendar')}} AS calendar
  ) as calendar on calendar.yearMonth = costs.yearMonth
  where costs.type is not null
