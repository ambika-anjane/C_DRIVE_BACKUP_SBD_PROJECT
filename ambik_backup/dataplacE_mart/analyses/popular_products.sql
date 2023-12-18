with SALES AS (
select C."COUNTRY_NAME",
       P."PRODUCT_NAME",
       SUM(F."SALES") as "SALES"
from {{ref('f_order_details')}} F,
     {{ref('d_country')}} C,
     {{ref('d_product')}} P
where C."COUNTRY_SK" = F."COUNTRY_SK" 
  and P."PRODUCT_SK" = F."PRODUCT_SK"
group by C."COUNTRY_NAME",
       P."PRODUCT_NAME"),
RANKED_SALES AS (
SELECT "COUNTRY_NAME",
       "PRODUCT_NAME",
       "SALES",
       RANK() OVER(PARTITION BY "COUNTRY_NAME" ORDER BY "SALES" DESC) RNK
FROM SALES)
-- GET TOP 5 PRODUCTS BY COUNTRIES
SELECT * 
FROM RANKED_SALES RS
WHERE RS.RNK <= 5;