{{config(materialized='table')}}
SELECT
 CONCAT(IFNULL(CAST(acquisition.date AS STRING),""),"",IFNULL(CAST(acquisition.source AS STRING),""),"",IFNULL(CAST(acquisition.medium AS STRING),""),"",IFNULL(CAST(acquisition.campaign AS STRING),""),"",IFNULL(CAST(acquisition.transactions AS STRING),""),"",IFNULL(CAST(acquisition.transactionRevenue AS STRING),"")) as join_key,
  IFNULL(acquisition.date, romi.date) date,
  IFNULL(ccg2.source, acquisition.source) source,
  IFNULL(ccg2.medium, acquisition.medium) medium,
  -- Validace kampaně: typ = media_type se propisuje z dat z reklamního sýstému, tedy kontroluju, že se jedná o data z reklamního systému a zároveň, že se jedná o reklamní systém (media_budget). Tím, že vím, že jedná o řádek z L2_ROMI_Marketing_Costs, tak potřebuju zkontrolovat, že se napároval na acquisition. Takže skusím propsat acquisition.campaign a pokud tam nic není, tak vím, že je potřeba něco opravit.
  IF(
    type = '{{var('ROMI_MEDIA_BUDGET')}}',
    IFNULL(
      acquisition.campaign,
      romi.campaign
    ),
    acquisition.campaign
  ) campaign,
  IF (acquisition.campaign is null, "Only ROMI", IF(romi.campaign is null, "Only GA", "Paired")) pair_status,
  acquisition.users,
  acquisition.sessions,
  acquisition.bounces,
  acquisition.sessionDuration,
  acquisition.transactions,
  acquisition.transactionRevenue,
  acquisition.account_name,
  acquisition.goal1,
  acquisition.goal2,
  acquisition.goal3,
  acquisition.goal4,
  acquisition.goal5,
  acquisition.goal6,
  acquisition.goal7,
  acquisition.goal8,
  acquisition.goal9,
  acquisition.goal10,
  acquisition.goal11,
  acquisition.goal12,
  acquisition.goal13,
  acquisition.goal14,
  acquisition.goal15,
  acquisition.goal16,
  acquisition.goal17,
  acquisition.goal18,
  acquisition.goal19,
  acquisition.goal20,
  IFNULL(ccg.ad_system, romi.ad_system) ad_system,
  romi.account,
  romi.account_id,
  romi.cost,
  romi.clicks,
  romi.impressions,
  romi.type,
  IFNULL(ccg2.channel_group, '{{var('ROMI_DEFAULT_CCG')}}') channel_group,

-- dopočet marže
--trans.order_margin_rate,
--trans.order_margin,
--trans.known_margin,
--trans.order_cancelled
--IF ((sum(trans.cancelled_orders) > 0), true, false) as order_cancelled,

FROM
  {{ref('L1_GA_Acquisition')}} acquisition
  LEFT JOIN {{source('spanario','L0_CCG')}} ccg USING(source, medium) 
  FULL OUTER JOIN {{ref('L2_ROMI_Marketing_Costs')}} romi USING(date, ad_system, campaign)
  LEFT JOIN {{source('spanario','L0_CCG')}} ccg2 USING(ad_system)

ORDER BY DATE DESC 
