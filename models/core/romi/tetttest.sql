SELECT
*

FROM
{{source('spanario','L0_GA_Acquisition')}}
WHERE date = '2022-01-22'