{{ config (materialized= 'table',tags= ["romi"])}}
-- v případě, že máš vypočítanou marži na R_Orders tak tohle můžeš použít.
SELECT
CONCAT(IFNULL(CAST(date AS STRING),""),
        "",
        IFNULL(CAST(source AS STRING),""),
        "",
        IFNULL(CAST(medium AS STRING),""),
        "",
        IFNULL(CAST(campaign AS STRING),""),
        "",
        IFNULL(CAST(transactions AS STRING),""),
        "",
        IFNULL(CAST(transactionRevenue AS STRING),"")
        ) as join_key,
date,
source,
medium,
campaign,
sum(transactions) as transactions,
sum(transactionRevenue) as transactionRevenue,
SUM(order_margin) / sum(transactionRevenue) as order_margin_rate,
SUM(order_margin) as order_margin,
--IF(SUM(order_margin) IS NULL,false,true) AS known_margin,
--order_cancelled,
sum(CASE WHEN order_cancelled = true then 1 else 0 end) as cancelled_orders,
FROM
  {{ref('L1_GA_acq_trans')}} acq

GROUP BY date, source, medium, campaign, join_key, order_cancelled
ORDER BY DATE DESC