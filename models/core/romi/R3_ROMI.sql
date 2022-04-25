 {{config(materialized='table')}}
 SELECT
 romi.* EXCEPT(join_key),
 trans.order_margin,
 trans.cancelled_orders,
 IF (trans.order_margin > 0, true, false) as known_margin
 FROM {{ref('R2_ROMI')}} romi
  --nově přidělané
LEFT JOIN (SELECT join_key, order_margin_rate, order_margin,cancelled_orders FROM {{ref('L3_GA_acq_trans')}}) trans USING (join_key)