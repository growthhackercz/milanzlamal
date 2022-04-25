-- v případě, že máš vypočítanou marži na R_Orders tak tohle můžeš použít.
SELECT
date,
transactionId,
source,
medium,
lower(campaign) as campaign,
transactions,
transactionRevenue,
--order_revenue, v případě, kdy nemáš správný transactionRevenue
order_cancelled,
ord.order_margin
FROM
  {{source('spanario','L0_acq-trans')}} acq
LEFT JOIN {{ref('R_Orders')}} ord ON ord.order_id = acq.transactionID
--WHERE date = '2022-04-21'
ORDER BY DATE DESC
