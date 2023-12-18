-- By Year & Month

select case when length(extract(month from "ORDER_DATE")::VARCHAR) < 2 then '0'||extract(month from "ORDER_DATE")::VARCHAR else extract(month from "ORDER_DATE")::VARCHAR END || '-' || TO_CHAR(DATE("ORDER_DATE"), 'Month') AS "MONTH",
       SUM(CASE WHEN extract(year from "ORDER_DATE") = 2019 THEN "SALES" ELSE 0 END) AS SALES_2019,
       SUM(CASE WHEN extract(year from "ORDER_DATE") = 2020 THEN "SALES" ELSE 0 END) AS SALES_2020,
       SUM(CASE WHEN extract(year from "ORDER_DATE") = 2021 THEN "SALES" ELSE 0 END) AS SALES_2021
from {{ref('f_order_details')}}
GROUP BY extract(month from "ORDER_DATE")
ORDER BY 1