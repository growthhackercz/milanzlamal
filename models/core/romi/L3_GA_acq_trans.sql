SELECT
* EXCEPT(join_key),
{{string_unify('join_key')}} as join_key
FROM {{ref('L2_GA_acq_trans')}}

