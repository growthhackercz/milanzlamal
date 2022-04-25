{{ config ( materialized= "incremental", tags= ["heureka"], unique_key= "sha256_hash", enabled=true )}} 

SELECT
  TO_HEX(
    SHA256(    
      CONCAT(
        IFNULL(CAST(itemIdShop AS STRING),""),
        ";",
        IFNULL(date,""),
        ";",
        IFNULL(CAST(shopId AS STRING),""),
        ";",
        IFNULL(CAST(productIdHeureka AS STRING),""),
        ";",
        IFNULL(source,"")
      )
    )
  )
as sha256_hash,
  *
FROM
  {{source('spanario','L0_Heureka_Costs')}}
