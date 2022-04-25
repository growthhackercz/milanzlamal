{{config(materialized="table",tags=["customer_analytics"])}}

WITH customer AS(
  SELECT
    inp.*,
    IF(inp.customer_orders > 1, 1, 0) customer_returning,
    
  FROM
    (
      SELECT
        customer_id,
        --CASE WHEN SUM (CAST((REPLACE (CAST(order_margin AS STRING),CAST(NULL AS STRING),CAST(order_margin AS STRING))) AS FLOAT64)) > 0  THEN true ELSE false END AS known_margin,
        MIN(order_date) AS customer_first_order,
        MAX(order_date) AS customer_latest_order,
        SUM(IFNULL(order_revenue, 0)) AS customer_total_revenue,
        AVG(IFNULL(order_revenue, 0)) AS customer_avg_revenue,
        SUM(1) AS customer_orders,
        NTILE(5) OVER (
          ORDER BY
            SUM(IFNULL(order_revenue, 0)) DESC
        ) AS customer_segment,
        NTILE(4) OVER (
          ORDER BY
            SUM(IFNULL(order_revenue, 0)) DESC
        ) AS rfm_monetary,
        NTILE(4) OVER (
          ORDER BY
            MAX(order_date) DESC
        ) AS rfm_recency,
        NTILE(4) OVER (
          ORDER BY
            SUM(1) DESC
        ) AS rfm_frequency
      FROM
        {{ref('L2_Orders')}}
      WHERE
        NOT order_cancelled
        AND customer_id IS NOT NULL
      GROUP BY
        customer_id
    ) inp
)
SELECT
  *,
  LAG(order_date) OVER (
    PARTITION BY customer_id
    ORDER BY
      order_index ASC
    ) AS preceding_order_date,
  DATE_DIFF(
    EXTRACT(
      DATE
      FROM
        order_date
    ),
    EXTRACT(
      DATE
      FROM
        LAG(order_date) OVER(
          PARTITION BY customer_id
          ORDER BY
            order_index ASC
        )
    ),
    DAY
  ) days_since_previous_order
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY orders.customer_id
        ORDER BY
          orders.order_date ASC
      ) AS order_index
    FROM
      {{ref('L2_Orders')}} orders
  )
  LEFT JOIN customer USING(customer_id)
  LEFT JOIN {{source('spanario','L0_RFM')}} rfm USING (rfm_monetary,rfm_frequency,rfm_recency)

--WHERE order_id = '2022000476'
ORDER BY order_id desc




/* This tranformation expects entry table "L1_Orders" with schema: 
  -- customer_id:STRING,
  -- customer_email:STRING,
  -- customer_phone:STRING,
  -- customer_name:STRING,
  -- acquisition_source:STRING,
  -- acquisition_medium:STRING,
  -- acquisition_campaign:STRING,
  -- order_id:STRING,
  -- order_date:TIMESTAMP,
  -- order_status:STRING,
  -- order_revenue:FLOAT,
  -- order_margin:FLOAT,
  -- order_cancelled:BOOLEAN,
  -- order_online:BOOLEAN,
  -- item_revenue:\FLOAT,
  -- known_margin:BOOLEAN

If there are no data available for any collumn set null value. 

Eg. You do not have GA data availabe. 

  */
