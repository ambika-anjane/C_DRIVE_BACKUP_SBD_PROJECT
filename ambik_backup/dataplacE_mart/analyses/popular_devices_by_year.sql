select C."COMPANY_NAME",
       UA."USER_AGENT",
       SUM(case when extract(year from F."ORDER_DATE") = 2019 then F."SALES" else 0 end) as SALES_2019,
       SUM(case when extract(year from F."ORDER_DATE") = 2020 then F."SALES" else 0 end) as SALES_2020,
       SUM(case when extract(year from F."ORDER_DATE") = 2021 then F."SALES" else 0 end) as SALES_2021
from {{ref('f_order_details')}} F,
     {{ref('d_company')}} C,
     {{ref('d_user_agent')}} UA
where C."COMPANY_SK" = F."COMPANY_SK" 
  and UA."USER_AGENT_SK" = F."USER_AGENT_SK"
group by C."COMPANY_NAME",
       UA."USER_AGENT";