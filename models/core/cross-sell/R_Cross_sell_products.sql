{{ config(materialized = 'table') }}

WITH total AS(
  SELECT
    document_date,
    CAST(COUNT(DISTINCT document_id) AS INT64) total
  FROM
    {{ref('L1_Cross_sell_products')}}
  GROUP BY
    document_date
),
products AS (
  SELECT
    product_name,
    document_date,
    count(distinct document_id) cnt
  FROM
    {{ref('L1_Cross_sell_products')}}
  GROUP BY
    product_name,
    document_date
),
combinations AS (
  SELECT
  a.document_date,
  a.product_name product_name,
  b.product_name product_name2,
  count(*) count
FROM
   {{ref('L1_Cross_sell_products')}} a FULL
  JOIN  {{ref('L1_Cross_sell_products')}} b USING(document_id)
WHERE
  a.product_name <> b.product_name
GROUP BY
  a.document_date,
  a.product_name,
  b.product_name
)
SELECT
  a.product_name,
  a.product_name2,
  a.document_date date,
  a.count,
  b.cnt product_count,
  c.cnt product_count2,
  total.total,
  -- a.count/total.total support_combination,
  -- b.cnt/total.total support_product,
  -- c.cnt/total.total support_product2,
  -- a.count/b.cnt confidence,
  -- (a.count/total.total)/((b.cnt/total.total)*(c.cnt/total.total)) lift
FROM
  combinations a FULL
  JOIN products b USING(product_name, document_date)
  LEFT JOIN total USING(document_date) FULL
  JOIN products c ON product_name2 = c.product_name
  AND c.document_date = a.document_date