select METRIC, "VALUE", trunc(sum(DW_SALES)::numeric ,2) DW_SALES, trunc(sum(ods_sales)::numeric ,2) ods_sales
from (
		-- By Year
		select 'YEAR' as METRIC,
		       extract(year from "ORDER_DATE")::VARCHAR AS "VALUE",
			   sum("SALES") as DW_SALES,
			   SUM(0) as ODS_SALES
		from {{ref('f_order_details')}}
		group by 1, 2
		union all 
		select 'YEAR' as METRIC,
		       extract(year from "ORDER_DATE")::VARCHAR AS "VALUE",
			   sum(0) as DW_SALES,
			   SUM(sale_price) as ODS_SALES
		from {{ source('marketplace', 'orders')}}
		group by 1, 2
		-- By Company
		union all
		select 'COMP' as METRIC,
		       dc."COMPANY_NAME"::VARCHAR AS "VALUE",
			   sum("SALES") as DW_SALES,
			   SUM(0) as ODS_SALES
		from {{ref('f_order_details')}} f left outer join {{ref('d_company')}} dc on dc."COMPANY_SK" = f."COMPANY_SK" 
		group by 1, 2
		union all 
		select 'COMP' as METRIC,
		       c.cname::VARCHAR AS "VALUE",
			   sum(0) as DW_SALES,
			   SUM(sale_price) as ODS_SALES
		from {{ source('marketplace', 'orders')}} o left outer join {{ source('marketplace', 'companies')}} c on c.cuit = o.company_id 
		group by 1, 2
		-- By Customer
		union all
		select 'CUST' as METRIC,
		       dc2."CUSTOMER_NAME"::VARCHAR AS "VALUE",
			   sum("SALES") as DW_SALES,
			   SUM(0) as ODS_SALES
		from {{ref('f_order_details')}} f left outer join {{ref('d_customer')}} dc2 on dc2."CUSTOMER_SK" = f."CUSTOMER_SK" 
		group by 1, 2
		union all 
		select 'CUST' as METRIC,
		       c2.full_name::VARCHAR AS "VALUE",
			   sum(0) as DW_SALES,
			   SUM(sale_price) as ODS_SALES
		from {{ source('marketplace', 'orders')}} o left outer join {{ source('marketplace', 'customers')}} c2 on c2.doc_nbr = o.customer_id 
		group by 1, 2
		-- By Product
		union all
		select 'PROD' as METRIC,
		       dp."PRODUCT_NAME"::VARCHAR AS "VALUE",
			   sum("SALES") as DW_SALES,
			   SUM(0) as ODS_SALES
		from {{ref('f_order_details')}} f left outer join {{ref('d_product')}} dp on dp."PRODUCT_SK" = f."PRODUCT_SK" 
		group by 1, 2
		union all 
		select 'PROD' as METRIC,
		       p.pname::VARCHAR AS "VALUE",
			   sum(0) as DW_SALES,
			   SUM(sale_price) as ODS_SALES
		from {{ source('marketplace', 'orders')}} o left outer join {{ source('marketplace', 'products')}} p on p.id = o.product_id 
		group by 1, 2
	) t
group by 1, 2
having trunc(sum(DW_SALES)::numeric ,2) <> trunc(sum(ods_sales)::numeric ,2)
;