SELECT
  /*CONCAT(IFNULL(CAST(acquisition.date AS STRING),""),
          "",
          IFNULL(CAST(acquisition.source AS STRING),""),
          "",
          IFNULL(CAST(acquisition.medium AS STRING),""),
          "",
          IFNULL(CAST(acquisition.campaign AS STRING),""),
          "",
          IFNULL(CAST(avg(acquisition.transactions) AS STRING),""),
          "",
          IFNULL(CAST(avg(acquisition.transactionRevenue) AS STRING),"")
          ) as join_key,*/
  acquisition.idProfile,
  CAST(acquisition.date AS DATE) AS date,
  acquisition.isoYearIsoWeek,
  LOWER(
    (
      CASE
        WHEN acquisition.source = "m.facebook.com" THEN 'facebook'
        WHEN acquisition.source = "l.facebook.com" THEN 'facebook'
        WHEN acquisition.source = "lm.facebook.com" THEN 'facebook'
        WHEN acquisition.source = "facebook.com" THEN 'facebook'
        WHEN acquisition.source = "instagram.com" THEN 'instagram'
        WHEN acquisition.source = "m.instagram.com" THEN 'instagram'
        WHEN acquisition.source = "l.instagram.com" THEN 'instagram'
        WHEN acquisition.source = "lm.instagram.com" THEN 'instagram'
        ELSE acquisition.source
      END
    )
  ) AS source,
  LOWER(acquisition.medium) AS medium,
  {{string_unify("acquisition.campaign")}} AS campaign,
  SUM(CAST(acquisition.users AS INT64)) AS users,
  SUM(CAST(acquisition.sessions AS INT64)) AS sessions,
  SUM(CAST(acquisition.bounces AS INT64)) AS bounces,
  SUM(CAST(acquisition.sessionDuration AS float64)) AS sessionDuration,
  SUM(CAST(acquisition.transactions AS INT64)) AS transactions,
  SUM(CAST(acquisition.transactionRevenue AS float64)) AS transactionRevenue,
  LOWER(accounts.name) AS account_name,
  SUM(CAST(goals10.goal1Completions AS INT64)) AS goal1,
  SUM(CAST(goals10.goal2Completions AS INT64)) AS goal2,
  SUM(CAST(goals10.goal3Completions AS INT64)) AS goal3,
  SUM(CAST(goals10.goal4Completions AS INT64)) AS goal4,
  SUM(CAST(goals10.goal5Completions AS INT64)) AS goal5,
  SUM(CAST(goals10.goal6Completions AS INT64)) AS goal6,
  SUM(CAST(goals10.goal7Completions AS INT64)) AS goal7,
  SUM(CAST(goals10.goal8Completions AS INT64)) AS goal8,
  SUM(CAST(goals10.goal9Completions AS INT64)) AS goal9,
  SUM(CAST(goals10.goal10Completions AS INT64)) AS goal10,
  SUM(CAST(goals20.goal11Completions AS INT64)) AS goal11,
  SUM(CAST(goals20.goal12Completions AS INT64)) AS goal12,
  SUM(CAST(goals20.goal13Completions AS INT64)) AS goal13,
  SUM(CAST(goals20.goal14Completions AS INT64)) AS goal14,
  SUM(CAST(goals20.goal15Completions AS INT64)) AS goal15,
  SUM(CAST(goals20.goal16Completions AS INT64)) AS goal16,
  SUM(CAST(goals20.goal17Completions AS INT64)) AS goal17,
  SUM(CAST(goals20.goal18Completions AS INT64)) AS goal18,
  SUM(CAST(goals20.goal19Completions AS INT64)) AS goal19,
  SUM(CAST(goals20.goal20Completions AS INT64)) AS goal20
FROM
  {{source('spanario','L0_GA_Acquisition')}} AS acquisition
  -- Join on account names.
  LEFT JOIN (
    SELECT
      accounts.id,
      accounts.name
    FROM
      {{source('spanario','L0_GA_Accounts')}} AS accounts
  ) as accounts on accounts.id = acquisition.idProfile
  -- Join on goals data for from 1 to 10. 
  LEFT JOIN (
    SELECT
       goals10.date,
       goals10.source,
       goals10.medium,
       goals10.campaign,
       goals10.goal1Completions,
       goals10.goal2Completions,
       goals10.goal3Completions,
       goals10.goal4Completions,
       goals10.goal5Completions,
       goals10.goal6Completions,
       goals10.goal7Completions,
       goals10.goal8Completions,
       goals10.goal9Completions,
       goals10.goal10Completions
    FROM
      {{source('spanario','L0_GA_Goals_10')}} AS goals10
  ) as goals10 on 
          goals10.date = acquisition.date AND 
          goals10.source = acquisition.source AND 
          goals10.medium = acquisition.medium AND 
          goals10.campaign = acquisition.campaign
  -- Join on goals data for from 11 to 20. 
  LEFT JOIN (
    SELECT
       goals20.date,
       goals20.source,
       goals20.medium,
       goals20.campaign,
       goals20.goal11Completions,
       goals20.goal12Completions,
       goals20.goal13Completions,
       goals20.goal14Completions,
       goals20.goal15Completions,
       goals20.goal16Completions,
       goals20.goal17Completions,
       goals20.goal18Completions,
       goals20.goal19Completions,
       goals20.goal20Completions
    FROM
      {{source('spanario','L0_GA_Goals_20')}} AS goals20
  ) as goals20 on 
          goals20.date = acquisition.date AND 
          goals20.source = acquisition.source AND 
          goals20.medium = acquisition.medium AND 
          goals20.campaign = acquisition.campaign

GROUP BY
  idProfile,
  acquisition.date,
  acquisition.campaign,
  isoYearIsoWeek,
  acquisition.source,
  acquisition.medium,
  name

ORDER BY DATE DESC