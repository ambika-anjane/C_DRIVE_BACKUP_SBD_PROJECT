with SALES AS (
select C."COMPANY_NAME",
       UA."USER_AGENT",
       SUM(F."SALES") as "SALES"
from {{ref('f_order_details')}} F,
     {{ref('d_company')}} C,
     {{ref('d_user_agent')}} UA
where C."COMPANY_SK" = F."COMPANY_SK" 
  and UA."USER_AGENT_SK" = F."USER_AGENT_SK"
group by C."COMPANY_NAME",
       UA."USER_AGENT"),
RANKED_SALES AS (
SELECT "COMPANY_NAME",
       "USER_AGENT",
       "SALES",
       RANK() OVER(PARTITION BY "COMPANY_NAME" ORDER BY "SALES" DESC) RNK
FROM SALES)
-- GET TOP 5 DEVICES BY B2B CLIENTS
SELECT * 
FROM RANKED_SALES RS
WHERE RS.RNK <= 5;
