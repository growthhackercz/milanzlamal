/* Template entry table for external table: 
https://docs.google.com/spreadsheets/d/1VLgHhrzCfjy_7pfh_YB7w-0kaGC8X8xGTyob0POMBG0/edit#gid=844664031
*/

SELECT * FROM
(SELECT 
"frequency" AS type,
rfm_frequency  AS segment,
CAST(MAX(customer_orders) AS STRING) AS max,
CAST(MIN(customer_orders) AS STRING) AS min,
COUNT(DISTINCT customer_id) AS customers
FROM {{ref('R_Orders')}} GROUP BY type, segment
UNION ALL
SELECT 
"recency" AS type,
rfm_recency  AS segment,
CAST(MAX(customer_latest_order) AS STRING) AS max,
CAST(MIN(customer_latest_order) AS STRING) AS min,
COUNT(DISTINCT customer_id) AS customers
FROM {{ref('R_Orders')}} GROUP BY type, segment
UNION ALL
SELECT 
"monetary" AS type,
rfm_monetary  AS segment,
CAST(MAX(customer_total_revenue) AS STRING) AS max,
CAST(MIN(customer_total_revenue) AS STRING) AS min,
COUNT(DISTINCT customer_id) AS customers
FROM {{ref('R_Orders')}} GROUP BY type, segment) WHERE segment IS NOT NULL
